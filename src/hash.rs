use blake2::Digest;
use std::fmt::Write;

#[derive(Clone)]
pub struct Hash([u8; 64]);

impl Hash {
    pub fn hash<R: std::io::Read>(mut r: R) -> std::io::Result<Self> {
        let mut hasher = blake2::Blake2b::new();
        std::io::copy(&mut r, &mut hasher)?;
        let mut hash = [0; 64];
        hash.copy_from_slice(&hasher.result()[..]);
        Ok(Self(hash))
    }

    pub fn to_string(&self) -> String {
        base64::encode(&self.0[..])
    }

    pub fn to_hex(&self) -> String {
        let mut s = String::new();
        for b in self.0.iter() {
            write!(&mut s, "{:02x}", b).unwrap();
        }
        s
    }
}

impl std::fmt::Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&self.to_string())
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&self.to_string())
    }
}

impl serde::Serialize for Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = base64::decode(&String::deserialize(deserializer)?).unwrap();
        let mut buf = [0; 64];
        buf.copy_from_slice(&data[0..64]);
        Ok(Self(buf))
    }
}
