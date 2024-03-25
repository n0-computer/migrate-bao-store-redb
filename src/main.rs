use std::path::PathBuf;

use clap::Parser;
use redb_15::ReadableTable;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long)]
    source: PathBuf,
    #[clap(long)]
    target: PathBuf,
}

/// Location of the data.
///
/// Data can be inlined in the database, a file conceptually owned by the store,
/// or a number of external files conceptually owned by the user.
///
/// Only complete data can be inlined.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum DataLocation<I = (), E = ()> {
    /// Data is in the inline_data table.
    Inline(I),
    /// Data is in the canonical location in the data directory.
    Owned(E),
    /// Data is in several external locations. This should be a non-empty list.
    External(Vec<PathBuf>, E),
}

/// Location of the outboard.
///
/// Outboard can be inlined in the database or a file conceptually owned by the store.
/// Outboards are implementation specific to the store and as such are always owned.
///
/// Only complete outboards can be inlined.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum OutboardLocation<I = ()> {
    /// Outboard is in the inline_outboard table.
    Inline(I),
    /// Outboard is in the canonical location in the data directory.
    Owned,
    /// Outboard is not needed
    NotNeeded,
}

/// The information about an entry that we keep in the entry table for quick access.
///
/// The exact info to store here is TBD, so usually you should use the accessor methods.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum EntryState<I = ()> {
    /// For a complete entry we always know the size. It does not make much sense
    /// to write to a complete entry, so they are much easier to share.
    Complete {
        /// Location of the data.
        data_location: DataLocation<I, u64>,
        /// Location of the outboard.
        outboard_location: OutboardLocation<I>,
    },
    /// Partial entries are entries for which we know the hash, but don't have
    /// all the data. They are created when syncing from somewhere else by hash.
    ///
    /// As such they are always owned. There is also no inline storage for them.
    /// Non short lived partial entries always live in the file system, and for
    /// short lived ones we never create a database entry in the first place.
    Partial {
        /// Once we get the last chunk of a partial entry, we have validated
        /// the size of the entry despite it still being incomplete.
        ///
        /// E.g. a giant file where we just requested the last chunk.
        size: Option<u64>,
    },
}

mod old {
    use super::{EntryState, SmallVec};
    use iroh_base_redb_15::hash::{Hash, HashAndFormat};
    use iroh_bytes_redb_15::Tag;
    use redb_15::TableDefinition;

    pub(super) const BLOBS_TABLE: TableDefinition<Hash, EntryState> =
        TableDefinition::new("blobs-0");

    pub(super) const TAGS_TABLE: TableDefinition<Tag, HashAndFormat> =
        TableDefinition::new("tags-0");

    pub(super) const INLINE_DATA_TABLE: TableDefinition<Hash, &[u8]> =
        TableDefinition::new("inline-data-0");

    pub(super) const INLINE_OUTBOARD_TABLE: TableDefinition<Hash, &[u8]> =
        TableDefinition::new("inline-outboard-0");

    impl redb_15::RedbValue for EntryState {
        type SelfType<'a> = EntryState;

        type AsBytes<'a> = SmallVec<[u8; 128]>;

        fn fixed_width() -> Option<usize> {
            None
        }

        fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
        where
            Self: 'a,
        {
            postcard::from_bytes(data).unwrap()
        }

        fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
        where
            Self: 'a,
            Self: 'b,
        {
            postcard::to_extend(value, SmallVec::new()).unwrap()
        }

        fn type_name() -> redb_15::TypeName {
            redb_15::TypeName::new("EntryState")
        }
    }
}

mod new {
    use super::{EntryState, SmallVec};
    use iroh_base::hash::{Hash, HashAndFormat};
    use iroh_bytes::Tag;
    use redb::TableDefinition;

    pub(super) const BLOBS_TABLE: TableDefinition<Hash, EntryState> =
        TableDefinition::new("blobs-0");

    pub(super) const TAGS_TABLE: TableDefinition<Tag, HashAndFormat> =
        TableDefinition::new("tags-0");

    pub(super) const INLINE_DATA_TABLE: TableDefinition<Hash, &[u8]> =
        TableDefinition::new("inline-data-0");

    pub(super) const INLINE_OUTBOARD_TABLE: TableDefinition<Hash, &[u8]> =
        TableDefinition::new("inline-outboard-0");

    impl redb::Value for EntryState {
        type SelfType<'a> = EntryState;

        type AsBytes<'a> = SmallVec<[u8; 128]>;

        fn fixed_width() -> Option<usize> {
            None
        }

        fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
        where
            Self: 'a,
        {
            postcard::from_bytes(data).unwrap()
        }

        fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
        where
            Self: 'a,
            Self: 'b,
        {
            postcard::to_extend(value, SmallVec::new()).unwrap()
        }

        fn type_name() -> redb::TypeName {
            redb::TypeName::new("EntryState")
        }
    }
}

fn convert_hash(old: iroh_base_redb_15::hash::Hash) -> iroh_base::hash::Hash {
    iroh_base::hash::Hash::from_bytes(*old.as_bytes())
}

fn convert_tag(old: iroh_bytes_redb_15::Tag) -> iroh_bytes::Tag {
    iroh_bytes::Tag(old.0)
}

fn convert_format(old: iroh_base_redb_15::hash::BlobFormat) -> iroh_base::hash::BlobFormat {
    match old {
        iroh_base_redb_15::hash::BlobFormat::Raw => iroh_base::hash::BlobFormat::Raw,
        iroh_base_redb_15::hash::BlobFormat::HashSeq => iroh_base::hash::BlobFormat::HashSeq,
    }
}

fn convert_hash_and_format(
    old: iroh_base_redb_15::hash::HashAndFormat,
) -> iroh_base::hash::HashAndFormat {
    iroh_base::hash::HashAndFormat {
        hash: convert_hash(old.hash),
        format: convert_format(old.format),
    }
}

fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let args = Args::parse();
    let old_db = redb_15::Database::open(args.source)?;
    if args.target.exists() {
        eprintln!("Target database already exists");
        std::process::exit(1);
    }
    let new_db = redb::Database::create(args.target)?;

    let rtx = old_db.begin_read()?;
    let wtx = new_db.begin_write()?;
    {
        let old_blobs = rtx.open_table(old::BLOBS_TABLE)?;
        let mut new_blobs = wtx.open_table(new::BLOBS_TABLE)?;
        for entry in old_blobs.iter()? {
            let (key, value) = entry?;
            let key = convert_hash(key.value());
            let value = value.value();
            tracing::info!("blobs {:?} {:?}", key, value);
            new_blobs.insert(key, value)?;
        }
        let old_tags = rtx.open_table(old::TAGS_TABLE)?;
        let mut new_tags = wtx.open_table(new::TAGS_TABLE)?;
        for entry in old_tags.iter()? {
            let (key, value) = entry?;
            let key = convert_tag(key.value());
            let value = convert_hash_and_format(value.value());
            tracing::info!("tags {:?} {:?}", key, value);
            new_tags.insert(key, value)?;
        }
        let old_inline_data = rtx.open_table(old::INLINE_DATA_TABLE)?;
        let mut new_inline_data = wtx.open_table(new::INLINE_DATA_TABLE)?;
        for entry in old_inline_data.iter()? {
            let (key, value) = entry?;
            let key = convert_hash(key.value());
            let value = value.value();
            tracing::info!("inline_data {:?} {:?}", key, value.len());
            new_inline_data.insert(key, value)?;
        }
        let old_inline_outboard = rtx.open_table(old::INLINE_OUTBOARD_TABLE)?;
        let mut new_inline_outboard = wtx.open_table(new::INLINE_OUTBOARD_TABLE)?;
        for entry in old_inline_outboard.iter()? {
            let (key, value) = entry?;
            let key = convert_hash(key.value());
            let value = value.value();
            tracing::info!("inline_outboard {:?} {:?}", key, value.len());
            new_inline_outboard.insert(key, value)?;
        }
    }
    wtx.commit()?;

    Ok(())
}
