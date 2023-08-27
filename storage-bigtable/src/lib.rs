#![allow(clippy::integer_arithmetic)]

use std::collections::HashSet;

use futures::Stream;
use itertools::Itertools;

use {
    crate::bigtable::RowKey,
    futures::StreamExt,
    futures::TryStreamExt,
    log::*,
    serde::{Deserialize, Serialize},
    solana_metrics::{datapoint_info, inc_new_counter_debug},
    solana_sdk::{
        clock::{Slot, UnixTimestamp},
        deserialize_utils::default_on_eof,
        message::v0::LoadedAddresses,
        pubkey::Pubkey,
        signature::Signature,
        sysvar::is_sysvar_id,
        transaction::{TransactionError, VersionedTransaction},
    },
    solana_storage_proto::convert::{generated, tx_by_addr},
    solana_transaction_status::{
        extract_and_fmt_memos, ConfirmedBlock, ConfirmedTransactionStatusWithSignature,
        ConfirmedTransactionWithStatusMeta, Reward, TransactionByAddrInfo,
        TransactionConfirmationStatus, TransactionStatus, TransactionStatusMeta,
        TransactionWithStatusMeta, VersionedConfirmedBlock, VersionedTransactionWithStatusMeta,
    },
    std::{collections::HashMap, convert::TryInto},
    thiserror::Error,
    tokio::task::JoinError,
};

#[macro_use]
extern crate solana_metrics;

#[macro_use]
extern crate serde_derive;

mod access_token;
pub mod bigtable;
mod compression;
mod root_ca_certificate;

#[derive(Debug, Error)]
pub enum Error {
    #[error("BigTable: {0}")]
    BigTableError(bigtable::Error),

    #[error("I/O Error: {0}")]
    IoError(std::io::Error),

    #[error("Transaction encoded is not supported")]
    UnsupportedTransactionEncoding,

    #[error("Block not found: {0}")]
    BlockNotFound(Slot),

    #[error("Signature not found")]
    SignatureNotFound,

    #[error("tokio error")]
    TokioJoinError(JoinError),

    #[error("argument error")]
    ArgumentError(String),
}

impl std::convert::From<bigtable::Error> for Error {
    fn from(err: bigtable::Error) -> Self {
        Self::BigTableError(err)
    }
}

impl std::convert::From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

// Convert a slot to its bucket representation whereby lower slots are always lexically ordered
// before higher slots
pub fn slot_to_key(slot: Slot) -> String {
    format!("{slot:016x}")
}

pub fn slot_to_blocks_key(slot: Slot) -> String {
    slot_to_key(slot)
}

pub fn slot_to_tx_by_addr_key(slot: Slot) -> String {
    slot_to_key(!slot)
}

// Reverse of `slot_to_key`
pub fn key_to_slot(key: &str) -> Option<Slot> {
    match Slot::from_str_radix(key, 16) {
        Ok(slot) => Some(slot),
        Err(err) => {
            // bucket data is probably corrupt
            warn!("Failed to parse object key as a slot: {}: {}", key, err);
            None
        }
    }
}

// A serialized `StoredConfirmedBlock` is stored in the `block` table
//
// StoredConfirmedBlock holds the same contents as ConfirmedBlock, but is slightly compressed and avoids
// some serde JSON directives that cause issues with bincode
//
// Note: in order to continue to support old bincode-serialized bigtable entries, if new fields are
// added to ConfirmedBlock, they must either be excluded or set to `default_on_eof` here
//
#[derive(Serialize, Deserialize)]
pub struct StoredConfirmedBlock {
    previous_blockhash: String,
    blockhash: String,
    parent_slot: Slot,
    transactions: Vec<StoredConfirmedBlockTransaction>,
    rewards: StoredConfirmedBlockRewards,
    block_time: Option<UnixTimestamp>,
    #[serde(deserialize_with = "default_on_eof")]
    block_height: Option<u64>,
}

#[cfg(test)]
impl From<ConfirmedBlock> for StoredConfirmedBlock {
    fn from(confirmed_block: ConfirmedBlock) -> Self {
        let ConfirmedBlock {
            previous_blockhash,
            blockhash,
            parent_slot,
            transactions,
            rewards,
            block_time,
            block_height,
        } = confirmed_block;

        Self {
            previous_blockhash,
            blockhash,
            parent_slot,
            transactions: transactions.into_iter().map(|tx| tx.into()).collect(),
            rewards: rewards.into_iter().map(|reward| reward.into()).collect(),
            block_time,
            block_height,
        }
    }
}

impl From<StoredConfirmedBlock> for ConfirmedBlock {
    fn from(confirmed_block: StoredConfirmedBlock) -> Self {
        let StoredConfirmedBlock {
            previous_blockhash,
            blockhash,
            parent_slot,
            transactions,
            rewards,
            block_time,
            block_height,
        } = confirmed_block;

        Self {
            previous_blockhash,
            blockhash,
            parent_slot,
            transactions: transactions.into_iter().map(|tx| tx.into()).collect(),
            rewards: rewards.into_iter().map(|reward| reward.into()).collect(),
            block_time,
            block_height,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct StoredConfirmedBlockTransaction {
    transaction: VersionedTransaction,
    meta: Option<StoredConfirmedBlockTransactionStatusMeta>,
}

#[cfg(test)]
impl From<TransactionWithStatusMeta> for StoredConfirmedBlockTransaction {
    fn from(value: TransactionWithStatusMeta) -> Self {
        match value {
            TransactionWithStatusMeta::MissingMetadata(transaction) => Self {
                transaction: VersionedTransaction::from(transaction),
                meta: None,
            },
            TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta {
                transaction,
                meta,
            }) => Self {
                transaction,
                meta: Some(meta.into()),
            },
        }
    }
}

impl From<StoredConfirmedBlockTransaction> for TransactionWithStatusMeta {
    fn from(tx_with_meta: StoredConfirmedBlockTransaction) -> Self {
        let StoredConfirmedBlockTransaction { transaction, meta } = tx_with_meta;
        match meta {
            None => Self::MissingMetadata(
                transaction
                    .into_legacy_transaction()
                    .expect("versioned transactions always have meta"),
            ),
            Some(meta) => Self::Complete(VersionedTransactionWithStatusMeta {
                transaction,
                meta: meta.into(),
            }),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct StoredConfirmedBlockTransactionStatusMeta {
    err: Option<TransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
}

impl From<StoredConfirmedBlockTransactionStatusMeta> for TransactionStatusMeta {
    fn from(value: StoredConfirmedBlockTransactionStatusMeta) -> Self {
        let StoredConfirmedBlockTransactionStatusMeta {
            err,
            fee,
            pre_balances,
            post_balances,
        } = value;
        let status = match &err {
            None => Ok(()),
            Some(err) => Err(err.clone()),
        };
        Self {
            status,
            fee,
            pre_balances,
            post_balances,
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: None,
            post_token_balances: None,
            rewards: None,
            loaded_addresses: LoadedAddresses::default(),
            return_data: None,
            compute_units_consumed: None,
        }
    }
}

impl From<TransactionStatusMeta> for StoredConfirmedBlockTransactionStatusMeta {
    fn from(value: TransactionStatusMeta) -> Self {
        let TransactionStatusMeta {
            status,
            fee,
            pre_balances,
            post_balances,
            ..
        } = value;
        Self {
            err: status.err(),
            fee,
            pre_balances,
            post_balances,
        }
    }
}

type StoredConfirmedBlockRewards = Vec<StoredConfirmedBlockReward>;

#[derive(Serialize, Deserialize)]
pub struct StoredConfirmedBlockReward {
    pubkey: String,
    lamports: i64,
}

impl From<StoredConfirmedBlockReward> for Reward {
    fn from(value: StoredConfirmedBlockReward) -> Self {
        let StoredConfirmedBlockReward { pubkey, lamports } = value;
        Self {
            pubkey,
            lamports,
            post_balance: 0,
            reward_type: None,
            commission: None,
        }
    }
}

impl From<Reward> for StoredConfirmedBlockReward {
    fn from(value: Reward) -> Self {
        let Reward {
            pubkey, lamports, ..
        } = value;
        Self { pubkey, lamports }
    }
}

// A serialized `TransactionInfo` is stored in the `tx` table
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct TransactionInfo {
    slot: Slot, // The slot that contains the block with this transaction in it
    index: u32, // Where the transaction is located in the block
    err: Option<TransactionError>, // None if the transaction executed successfully
    memo: Option<String>, // Transaction memo
}

// Part of a serialized `TransactionInfo` which is stored in the `tx` table
#[derive(PartialEq, Eq, Debug)]
pub struct UploadedTransaction {
    slot: Slot, // The slot that contains the block with this transaction in it
    index: u32, // Where the transaction is located in the block
    err: Option<TransactionError>, // None if the transaction executed successfully
}

impl From<TransactionInfo> for UploadedTransaction {
    fn from(transaction_info: TransactionInfo) -> Self {
        Self {
            slot: transaction_info.slot,
            index: transaction_info.index,
            err: transaction_info.err,
        }
    }
}

impl From<TransactionInfo> for TransactionStatus {
    fn from(transaction_info: TransactionInfo) -> Self {
        let TransactionInfo { slot, err, .. } = transaction_info;
        let status = match &err {
            None => Ok(()),
            Some(err) => Err(err.clone()),
        };
        Self {
            slot,
            confirmations: None,
            status,
            err,
            confirmation_status: Some(TransactionConfirmationStatus::Finalized),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyTransactionByAddrInfo {
    pub signature: Signature,          // The transaction signature
    pub err: Option<TransactionError>, // None if the transaction executed successfully
    pub index: u32,                    // Where the transaction is located in the block
    pub memo: Option<String>,          // Transaction memo
}

impl From<LegacyTransactionByAddrInfo> for TransactionByAddrInfo {
    fn from(legacy: LegacyTransactionByAddrInfo) -> Self {
        let LegacyTransactionByAddrInfo {
            signature,
            err,
            index,
            memo,
        } = legacy;

        Self {
            signature,
            err,
            index,
            memo,
            block_time: None,
        }
    }
}

pub const DEFAULT_INSTANCE_NAME: &str = "solana-ledger";
pub const DEFAULT_APP_PROFILE_ID: &str = "default";
pub const DEFAULT_WINDOW_SIZE: u32 = 52428800;

#[derive(Debug)]
pub enum CredentialType {
    Filepath(Option<String>),
    Stringified(String),
}

#[derive(Debug)]
pub struct LedgerStorageConfig {
    pub read_only: bool,
    pub timeout: Option<std::time::Duration>,
    pub instance_name: String,
    pub app_profile_id: String,
    pub project: String,
    pub stream_window_size: u32,
    pub connection_window_size: u32,
    pub credential_type: CredentialType,
}

impl Default for LedgerStorageConfig {
    fn default() -> Self {
        Self {
            read_only: true,
            timeout: None,
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
            app_profile_id: DEFAULT_APP_PROFILE_ID.to_string(),
            project: "none".to_string(),
            stream_window_size: DEFAULT_WINDOW_SIZE,
            connection_window_size: DEFAULT_WINDOW_SIZE,
            credential_type: CredentialType::Filepath(None),
        }
    }
}

#[derive(Clone)]
pub struct LedgerStorage {
    connection: bigtable::BigTableConnection,
}

impl LedgerStorage {
    pub async fn new(
        read_only: bool,
        timeout: Option<std::time::Duration>,
        project: String,
        stream_window_size: u32,
        connection_window_size: u32,
        credential_path: Option<String>,
    ) -> Result<Self> {
        Self::new_with_config(LedgerStorageConfig {
            read_only,
            timeout,
            project,
            stream_window_size,
            connection_window_size,
            credential_type: CredentialType::Filepath(credential_path),
            ..LedgerStorageConfig::default()
        })
        .await
    }

    pub async fn new_with_config(config: LedgerStorageConfig) -> Result<Self> {
        let LedgerStorageConfig {
            read_only,
            timeout,
            instance_name,
            app_profile_id,
            project,
            stream_window_size,
            connection_window_size,
            credential_type,
        } = config;
        let connection = bigtable::BigTableConnection::new(
            instance_name.as_str(),
            app_profile_id.as_str(),
            project.as_str(),
            read_only,
            timeout,
            stream_window_size,
            connection_window_size,
            credential_type,
        )
        .await?;
        Ok(Self { connection })
    }

    pub async fn new_with_project(project: String) -> Result<Self> {
        Self::new_with_config(LedgerStorageConfig {
            project,
            ..LedgerStorageConfig::default()
        })
        .await
    }

    /// Return the available slot that contains a block
    pub async fn get_first_available_block(&self) -> Result<Option<Slot>> {
        debug!("LedgerStorage::get_first_available_block request received");
        inc_new_counter_debug!("storage-bigtable-query", 1);
        let mut bigtable = self.connection.client();
        let blocks = bigtable.get_row_keys("blocks", None, None, 1).await?;
        futures::pin_mut!(blocks);
        let has_next = blocks.try_next().await?;
        Ok(has_next.and_then(|a| key_to_slot(&a)))
    }

    /// Fetch the next slots after the provided slot that contains a block
    ///
    /// start_slot: slot to start the search from (inclusive)
    /// limit: stop after this many slots have been found
    pub async fn get_confirmed_blocks<'a>(
        &'a self,
        start_slot: Slot,
        limit: usize,
    ) -> impl Stream<Item = Result<Slot>> + 'a {
        debug!(
            "LedgerStorage::get_confirmed_blocks request received: {:?} {:?}",
            start_slot, limit
        );
        inc_new_counter_debug!("storage-bigtable-query", 1);
        let mut bigtable = self.connection.client();

        async_stream::stream! {
            let blocks_result = bigtable
                .get_row_keys(
                    "blocks",
                    Some(slot_to_blocks_key(start_slot)),
                    None,
                    limit as i64,
                )
                .await;
            if blocks_result.is_err() {
                yield Err(Error::BigTableError(blocks_result.err().unwrap()));
                return;
            }
            let blocks = blocks_result.unwrap();
            let blocks = blocks
                .filter_map(|s| async {
                    match s {
                        Ok(s) => key_to_slot(&s).and_then(|s| Some(Ok(s))),
                        Err(e) => Some(Err(Error::BigTableError(e))),
                    }
                });
            for await block in blocks {
                yield block;
            }
        }
    }

    // Fetches and gets a vector of confirmed blocks via a multirow fetch
    pub async fn get_confirmed_blocks_with_data<'a>(
        &'a self,
        slots: &'a [Slot],
    ) -> impl Stream<Item = Result<(Slot, ConfirmedBlock)>> + 'a {
        debug!(
            "LedgerStorage::get_confirmed_blocks_with_data request received: {:?}",
            slots
        );
        inc_new_counter_debug!("storage-bigtable-query", 1);

        async_stream::stream! {
            let mut bigtable = self.connection.client();
            let row_keys = slots.iter().copied().map(slot_to_blocks_key);
            let data = bigtable
                .get_protobuf_or_bincode_cells("blocks", row_keys)
                .await;
            if data.is_err() {
                yield Err(Error::BigTableError(data.err().unwrap()));
                return;
            }
            let data = data.unwrap();
            let data = data
                .map_err(|e| Error::BigTableError(e))
                .map(
                    |result|
                        result.and_then(|(row_key, block_cell_data): (
                            RowKey,
                            bigtable::CellData<StoredConfirmedBlock, generated::ConfirmedBlock>,
                        )| {
                            let block: ConfirmedBlock = match block_cell_data {
                                bigtable::CellData::Bincode(block) => block.into(),
                                bigtable::CellData::Protobuf(block) =>
                                    block.try_into()
                                        .map_err(|e: Box<bincode::ErrorKind>| Error::BigTableError(bigtable::Error::ObjectCorrupt(e.to_string()))
                                )?
                            };
                            Ok((key_to_slot(&row_key).unwrap(), block))
                        }
                    )
                );
            for await datum in data {
                yield datum;
            }
        }
    }

    /// Fetch the confirmed block from the desired slot
    pub async fn get_confirmed_block(&self, slot: Slot) -> Result<ConfirmedBlock> {
        debug!(
            "LedgerStorage::get_confirmed_block request received: {:?}",
            slot
        );
        inc_new_counter_debug!("storage-bigtable-query", 1);
        let mut bigtable = self.connection.client();
        let block_cell_data = bigtable
            .get_protobuf_or_bincode_cell::<StoredConfirmedBlock, generated::ConfirmedBlock>(
                "blocks",
                slot_to_blocks_key(slot),
            )
            .await
            .map_err(|err| match err {
                bigtable::Error::RowNotFound => Error::BlockNotFound(slot),
                _ => err.into(),
            })?;
        Ok(match block_cell_data {
            bigtable::CellData::Bincode(block) => block.into(),
            bigtable::CellData::Protobuf(block) => block.try_into().map_err(|_err| {
                bigtable::Error::ObjectCorrupt(format!("blocks/{}", slot_to_blocks_key(slot)))
            })?,
        })
    }

    pub async fn get_signature_status(&self, signature: &Signature) -> Result<TransactionStatus> {
        debug!(
            "LedgerStorage::get_signature_status request received: {:?}",
            signature
        );
        inc_new_counter_debug!("storage-bigtable-query", 1);
        let mut bigtable = self.connection.client();
        let transaction_info = bigtable
            .get_bincode_cell::<TransactionInfo>("tx", signature.to_string())
            .await
            .map_err(|err| match err {
                bigtable::Error::RowNotFound => Error::SignatureNotFound,
                _ => err.into(),
            })?;
        Ok(transaction_info.into())
    }

    // Fetches and gets a vector of confirmed transactions via a multirow fetch
    // doesnt use streaming. Also returns as array of missing txs.
    pub async fn get_confirmed_transactions(
        &self,
        signatures: &[Signature],
    ) -> Result<(Vec<ConfirmedTransactionWithStatusMeta>, Vec<String>)> {
        debug!(
            "LedgerStorage::get_confirmed_transactions request received: {:?}",
            signatures
        );
        inc_new_counter_debug!("storage-bigtable-query", 1);
        let mut bigtable = self.connection.client();

        // Fetch transactions info
        let keys = signatures.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let cells = bigtable
            .get_bincode_cells::<TransactionInfo>("tx", &keys)
            .await?
            .map(|cell| match cell {
                Ok((sig, Ok(TransactionInfo { slot, index, .. }))) => {
                    Ok((sig, Some((slot, index))))
                }
                Ok((sig, Err(e))) => {
                    //I thought this hit on missing tx, but it doesn't
                    warn!("Error looking up transaction info for {}: {:?}", sig, e);
                    Ok((sig, None))
                }
                Err(e) => Err(e),
            })
            .try_collect::<Vec<_>>()
            .await?;
        //.await?;

        let mut good_cells: Vec<(Slot, u32)> = Vec::new();
        let mut bad_cells: Vec<String> = Vec::new();

        for cell in &cells {
            match cell {
                (_, Some(c)) => good_cells.push(*c),
                (s, None) => bad_cells.push(s.clone()),
            }
        }

        let txs = self
            .get_confirmed_transactions_by_slot_index(good_cells)
            .await?;
        let mut good_txs: Vec<ConfirmedTransactionWithStatusMeta> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for (i, tx) in txs.into_iter().enumerate() {
            seen.insert(cells[i].0.clone());
            match tx {
                Some(t) => {
                    good_txs.push(t);
                }
                None => bad_cells.push(cells[i].0.clone()),
            }
        }
        //add to bad cells any missing results
        bad_cells.extend(keys.into_iter().filter(|k| !seen.contains(k)));

        Ok((good_txs, bad_cells))
    }

    pub async fn get_confirmed_transactions_by_slot_index(
        &self,
        slot_indexes: Vec<(Slot, u32)>,
    ) -> Result<Vec<Option<ConfirmedTransactionWithStatusMeta>>> {
        debug!(
            "LedgerStorage::get_confirmed_transactions request received: {:?}",
            slot_indexes
        );
        let slots_vec = slot_indexes
            .iter()
            .map(|a| a.0)
            .unique()
            .collect::<Vec<_>>();
        // Fetch blocks
        let blocks = self.get_confirmed_blocks_with_data(&slots_vec).await;

        let mut blocks_map: HashMap<Slot, ConfirmedBlock> = HashMap::new();
        futures::pin_mut!(blocks); // needed for iteration
        while let Some(block) = blocks.next().await {
            let block = block?;
            blocks_map.insert(block.0, block.1);
        }

        // Extract transactions
        Ok(slot_indexes
            .into_iter()
            .map(|(slot, index)| {
                blocks_map.get(&slot).and_then(|block| {
                    block.transactions.get(index as usize).map(|tx_with_meta| {
                        ConfirmedTransactionWithStatusMeta {
                            slot,
                            tx_with_meta: tx_with_meta.clone(),
                            block_time: block.block_time,
                        }
                    })
                })
            })
            .collect::<Vec<_>>())
    }

    /// Fetch a confirmed transaction
    pub async fn get_confirmed_transaction(
        &self,
        signature: &Signature,
    ) -> Result<Option<ConfirmedTransactionWithStatusMeta>> {
        debug!(
            "LedgerStorage::get_confirmed_transaction request received: {:?}",
            signature
        );
        inc_new_counter_debug!("storage-bigtable-query", 1);

        // Figure out which block the transaction is located in
        match self.get_slot_for_signature(signature).await {
            Ok((slot, index)) => {
                self.get_confirmed_transaction_by_slot_index(slot, index)
                    .await
            }
            Err(Error::SignatureNotFound) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Fetch a confirmed transaction
    pub async fn get_confirmed_transaction_by_slot_index(
        &self,
        slot: u64,
        index: u32,
    ) -> Result<Option<ConfirmedTransactionWithStatusMeta>> {
        debug!(
            "LedgerStorage::get_confirmed_transaction_by_slot_index request received: {} {}",
            slot, index,
        );
        // Load the block and return the transaction
        let block = self.get_confirmed_block(slot).await?;
        match block.transactions.into_iter().nth(index as usize) {
            None => {
                // report this somewhere actionable?
                warn!("Transaction info for {:?} is corrupt", (slot, index));
                Ok(None)
            }
            Some(tx_with_meta) => Ok(Some(ConfirmedTransactionWithStatusMeta {
                slot,
                tx_with_meta,
                block_time: block.block_time,
            })),
        }
    }

    pub async fn get_slot_for_signature<'a>(
        &'a self,
        signature: &Signature,
    ) -> Result<(Slot, u32)> {
        debug!(
            "LedgerStorage::get_slot_for_signature request received: {:?}",
            signature
        );
        inc_new_counter_debug!("storage-bigtable-query", 1);
        let mut bigtable = self.connection.client();
        let TransactionInfo { slot, index, .. } = bigtable
            .get_bincode_cell::<TransactionInfo>("tx", signature.to_string())
            .await
            .map_err(|err| match err {
                bigtable::Error::RowNotFound => Error::SignatureNotFound,
                _ => err.into(),
            })?;
        Ok((slot, index))
    }

    /// Get confirmed signatures for the provided address, in descending ledger order
    ///
    /// address: address to search for
    /// before_signature: start with the first signature older than this one
    /// until_signature: end with the last signature more recent than this one
    /// limit: stop after this many signatures; if limit==0, all records in the table will be read
    pub async fn get_confirmed_signatures_for_address<'a>(
        &'a self,
        address: Pubkey,
        before_signature: Option<Signature>,
        before_slot: Option<Slot>,
    ) -> Result<
        impl Stream<
                Item = Result<(
                    ConfirmedTransactionStatusWithSignature,
                    u32, /*slot index*/
                )>,
            > + 'a,
    > {
        if before_slot.is_some() && before_signature.is_some() {
            return Err(Error::ArgumentError(
                "before_slot and before_signature cannot be used together".to_string(),
            ));
        }

        debug!(
            "LedgerStorage::get_confirmed_signatures_for_address request received: {:?}",
            address
        );
        inc_new_counter_debug!("storage-bigtable-query", 1);

        let stream = async_stream::stream! {
            let mut bigtable = self.connection.client();
            let address_prefix = format!("{address}/");

            // Figure out where to start listing from based on `before_signature`
            let (first_slot, before_transaction_index) = match (before_signature, before_slot) {
                (None, None) => (Slot::MAX, 0),
                (Some(before_signature), None) => {
                    self.get_slot_for_signature(&before_signature).await?
                },
                (None, Some(before_slot)) => {
                    (before_slot, u32::MAX)
                },
                (Some(_), Some(_)) => unreachable!("Both before_signature and before_slot are set"),
            };

            // Return the next tx-by-addr data of amount `limit` plus extra to account for the largest
            // number that might be flitered out
            let tx_by_addr_data = bigtable
                .get_row_data(
                    "tx-by-addr",
                    Some(format!(
                        "{}{}",
                        address_prefix,
                        slot_to_tx_by_addr_key(first_slot),
                    )),
                    Some(format!(
                        "{}{}",
                        address_prefix,
                        slot_to_tx_by_addr_key(0),
                    )),
                    i64::MAX,
                )
                .await?;

            futures::pin_mut!(tx_by_addr_data);
            while let Some(record) = tx_by_addr_data.next().await {
                let (row_key, data) = record?;

                let slot = !key_to_slot(&row_key[address_prefix.len()..]).ok_or_else(|| {
                    bigtable::Error::ObjectCorrupt(format!(
                        "Failed to convert key to slot: tx-by-addr/{row_key}"
                    ))
                })?;

                let deserialized_cell_data = bigtable::deserialize_protobuf_or_bincode_cell_data::<
                    Vec<LegacyTransactionByAddrInfo>,
                    tx_by_addr::TransactionByAddr,
                >(&data, "tx-by-addr", row_key.clone())?;

                let mut cell_data: Vec<TransactionByAddrInfo> = match deserialized_cell_data {
                    bigtable::CellData::Bincode(tx_by_addr) => {
                        tx_by_addr.into_iter().map(|legacy| legacy.into()).collect()
                    }
                    bigtable::CellData::Protobuf(tx_by_addr) => {
                        tx_by_addr.try_into().map_err(|error| {
                            bigtable::Error::ObjectCorrupt(format!(
                                "Failed to deserialize: {}: tx-by-addr/{}",
                                error,
                                row_key.clone()
                            ))
                        })?
                    }
                };

                cell_data.reverse();
                for tx_by_addr_info in cell_data.into_iter() {
                    // Filter out records before `before_transaction_index`
                    if slot == first_slot && tx_by_addr_info.index >= before_transaction_index {
                        continue;
                    }
                    yield Ok((
                        ConfirmedTransactionStatusWithSignature {
                            signature: tx_by_addr_info.signature,
                            slot,
                            err: tx_by_addr_info.err,
                            memo: tx_by_addr_info.memo,
                            block_time: tx_by_addr_info.block_time,
                        },
                        tx_by_addr_info.index,
                    ));
                }
            }
        };
        Ok(stream)
    }

    // Upload a new confirmed block and associated meta data.
    pub async fn upload_confirmed_block(
        &self,
        slot: Slot,
        confirmed_block: VersionedConfirmedBlock,
    ) -> Result<()> {
        let mut by_addr: HashMap<&Pubkey, Vec<TransactionByAddrInfo>> = HashMap::new();

        let mut tx_cells = vec![];
        for (index, transaction_with_meta) in confirmed_block.transactions.iter().enumerate() {
            let VersionedTransactionWithStatusMeta { meta, transaction } = transaction_with_meta;
            let err = meta.status.clone().err();
            let index = index as u32;
            let signature = transaction.signatures[0];
            let memo = extract_and_fmt_memos(transaction_with_meta);

            for address in transaction_with_meta.account_keys().iter() {
                if !is_sysvar_id(address) {
                    by_addr
                        .entry(address)
                        .or_default()
                        .push(TransactionByAddrInfo {
                            signature,
                            err: err.clone(),
                            index,
                            memo: memo.clone(),
                            block_time: confirmed_block.block_time,
                        });
                }
            }

            tx_cells.push((
                signature.to_string(),
                TransactionInfo {
                    slot,
                    index,
                    err,
                    memo,
                },
            ));
        }

        let tx_by_addr_cells: Vec<_> = by_addr
            .into_iter()
            .map(|(address, transaction_info_by_addr)| {
                (
                    format!("{}/{}", address, slot_to_tx_by_addr_key(slot)),
                    tx_by_addr::TransactionByAddr {
                        tx_by_addrs: transaction_info_by_addr
                            .into_iter()
                            .map(|by_addr| by_addr.into())
                            .collect(),
                    },
                )
            })
            .collect();

        let mut tasks = vec![];

        if !tx_cells.is_empty() {
            let conn = self.connection.clone();
            tasks.push(tokio::spawn(async move {
                conn.put_bincode_cells_with_retry::<TransactionInfo>("tx", &tx_cells)
                    .await
            }));
        }

        if !tx_by_addr_cells.is_empty() {
            let conn = self.connection.clone();
            tasks.push(tokio::spawn(async move {
                conn.put_protobuf_cells_with_retry::<tx_by_addr::TransactionByAddr>(
                    "tx-by-addr",
                    &tx_by_addr_cells,
                )
                .await
            }));
        }

        let mut bytes_written = 0;
        let mut maybe_first_err: Option<Error> = None;

        let results = futures::future::join_all(tasks).await;
        for result in results {
            match result {
                Err(err) => {
                    if maybe_first_err.is_none() {
                        maybe_first_err = Some(Error::TokioJoinError(err));
                    }
                }
                Ok(Err(err)) => {
                    if maybe_first_err.is_none() {
                        maybe_first_err = Some(Error::BigTableError(err));
                    }
                }
                Ok(Ok(bytes)) => {
                    bytes_written += bytes;
                }
            }
        }

        if let Some(err) = maybe_first_err {
            return Err(err);
        }

        let num_transactions = confirmed_block.transactions.len();

        // Store the block itself last, after all other metadata about the block has been
        // successfully stored.  This avoids partial uploaded blocks from becoming visible to
        // `get_confirmed_block()` and `get_confirmed_blocks()`
        let blocks_cells = [(slot_to_blocks_key(slot), confirmed_block.into())];
        bytes_written += self
            .connection
            .put_protobuf_cells_with_retry::<generated::ConfirmedBlock>("blocks", &blocks_cells)
            .await?;
        datapoint_info!(
            "storage-bigtable-upload-block",
            ("slot", slot, i64),
            ("transactions", num_transactions, i64),
            ("bytes", bytes_written, i64),
        );
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_slot_to_key() {
        assert_eq!(slot_to_key(0), "0000000000000000");
        assert_eq!(slot_to_key(!0), "ffffffffffffffff");
    }
}
