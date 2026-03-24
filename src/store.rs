use crate::error::{Error, Result};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, 
    BufWriter, 
    Read, 
    Write};
use std::path::Path;
use std::sync::RwLock;

/// On-disk snapshot format (version 1)
///
/// ```text
/// [4]  magic       "KVS1"
/// [4]  entry_count u32 le
/// for each entry:
///   [1]  key_len   u8          (max 255)
///   [N]  key       utf-8 bytes
///   [4]  val_len   u32 le
///   [M]  value     raw bytes
/// ```
pub struct Store {
    data: RwLock<HashMap<String, Vec<u8>>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: RwLock
            ::new(HashMap
                ::new()),
        }
    }

    pub fn set(
        &self,
        key: String,
        value: Vec<u8>,
    ) -> Result<()> {
        self.data
            .write()
            .unwrap()
            .insert(key, value);
        Ok(())
    }

    pub fn get(
        &self,
        key: &str,
    ) -> Result<Option<Vec<u8>>> {
        Ok(self.data
            .read()
            .unwrap()
            .get(key)
            .cloned())
    }

    /// Returns `true` if the key existed and was removed.
    pub fn delete(
        &self,
        key: &str,
    ) -> Result<bool> {
        Ok(self.data
            .write()
            .unwrap()
            .remove(key)
            .is_some())
    }

    /// Writes a snapshot atomically: serialises to a sibling `.tmp` file,
    /// then renames into place. A crash mid-write leaves the previous
    /// snapshot intact.
    pub fn save_snapshot(
        &self,
        path: &Path,
    ) -> Result<()> {
        // Clone under the lock so we release it before doing any I/O.
        // Holding a read lock across disk writes would block every Set/Delete
        // for the full duration of the flush.
        let snapshot = self
                    .data
                    .read()
                    .unwrap()
                    .clone();

        let tmp = path.with_extension("tmp");
        {
            let mut w = BufWriter::new(File::create(&tmp)?);
            w.write_all(b"KVS1")?;
            let count = u32::try_from(snapshot.len())
                .map_err(|_| Error
                    ::Store("too many keys for snapshot format".into()))?;
            w.write_all(&count.to_le_bytes())?;
            for (key, value) in &snapshot {
                let key_len = u8::try_from(key.len())
                    .map_err(|_| Error
                        ::Store(format!("key too long: {}", key)))?;
                write_u8(&mut w, key_len)?;
                w.write_all(key.as_bytes())?;
                let val_len = u32::try_from(value.len())
                    .map_err(|_| Error
                        ::Store("value too large for snapshot format".into()))?;
                w.write_all(&val_len.to_le_bytes())?;
                w.write_all(value)?;
            }
            w.flush()?;
            w.into_inner()
                .map_err(|e| Error::Store(e.to_string()))?
                .sync_all()?;
        }
        // Atomic promotion — either the full file is visible or the old one is.
        fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Replaces the current store contents with a snapshot from `path`.
    pub fn load_snapshot(
        &self,
        path: &Path,
    ) -> Result<()> {
        let mut r = BufReader::new(File::open(path)?);

        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if &magic != b"KVS1" {
            return Err(Error::Store(format!(
                "unrecognised snapshot magic {magic:?}; expected b\"KVS1\""
            )));
        }

        let count = read_u32_le(&mut r)? as usize;
        let mut map = HashMap::with_capacity(count);

        for _ in 0..count {
            let key_len = read_u8(&mut r)? as usize;
            let mut key_bytes = vec![0u8; key_len];
            r.read_exact(&mut key_bytes)?;
            let key = String::from_utf8(key_bytes)
                .map_err(|e| Error::Store(format!("invalid UTF-8 in key: {e}")))?;

            let val_len = read_u32_le(&mut r)? as usize;
            let mut value = vec![0u8; val_len];
            r.read_exact(&mut value)?;

            map.insert(key, value);
        }

        *self.data.write().unwrap() = map;
        Ok(())
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

/// I/O primitives

fn read_u8<R: Read>(
    r: &mut R,
) -> Result<u8> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(b[0])
}

fn write_u8<W: Write>(
    w: &mut W,
    b: u8,
) -> Result<()> {
    w.write_all(&[b]).map_err(Into::into)
}

fn read_u32_le<R: Read>(
    r: &mut R,
) -> Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

// ── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn populated_store() -> Store {
        let s = Store::new();
        s.set("k1".into(), b"v1".to_vec()).unwrap();
        s.set("k2".into(), b"v2".to_vec()).unwrap();
        s
    }

    #[test]
    fn get_returns_none_for_missing_key() {
        assert_eq!(
            Store::new().get("nope").unwrap(),
            None
        );
    }

    #[test]
    fn set_overwrites_existing_key() {
        let s = Store::new();
        s.set("k".into(), b"old".to_vec()).unwrap();
        s.set("k".into(), b"new".to_vec()).unwrap();
        assert_eq!(
            s.get("k").unwrap(),
            Some(b"new".to_vec())
        );
    }

    #[test]
    fn delete_returns_true_only_when_key_existed() {
        let s = Store::new();
        s.set("k".into(), b"v".to_vec()).unwrap();
        assert!(s.delete("k").unwrap());
        assert!(!s.delete("k").unwrap()); // already gone
    }

    #[test]
    fn snapshot_roundtrip_preserves_all_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dump.rdb");
        let store = populated_store();
        store.save_snapshot(&path).unwrap();

        let loaded = Store::new();
        loaded.load_snapshot(&path).unwrap();
        assert_eq!(
            loaded.get("k1").unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            loaded.get("k2").unwrap(),
            Some(b"v2".to_vec())
        );
    }

    #[test]
    fn snapshot_is_atomic_tmp_file_is_cleaned_up() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dump.rdb");
        populated_store().save_snapshot(&path).unwrap();
        assert!(path.exists());
        assert!(!path.with_extension("tmp").exists()); // no leftover .tmp
    }

    #[test]
    fn load_rejects_bad_magic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.rdb");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"JUNK")
            .unwrap();
        assert!(
            Store::new()
                .load_snapshot(&path)
                .is_err()
        );
    }

    #[test]
    fn load_replaces_existing_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dump.rdb");
        populated_store().save_snapshot(&path).unwrap();

        // Pre-populate the target store with a stale key.
        let s = Store::new();
        s.set("stale".into(), b"gone".to_vec()).unwrap();
        s.load_snapshot(&path).unwrap();

        assert_eq!(s.get("stale").unwrap(), None);
        assert_eq!(
            s.get("k1").unwrap(),
            Some(b"v1".to_vec())
        );
    }
}