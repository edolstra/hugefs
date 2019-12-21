use aes_ctr::stream_cipher::generic_array::GenericArray;
use blake2::Digest;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Hash(pub GenericArray<u8, <blake2::Blake2b as Digest>::OutputSize>);

impl Hash {
    pub fn hash<R: std::io::Read>(mut r: R) -> std::io::Result<(u64, Self)> {
        let mut hasher = blake2::Blake2b::new();
        let n = std::io::copy(&mut r, &mut hasher)?;
        Ok((n, Self(hasher.result())))
    }

    pub fn from_hex(s: &str) -> Self {
        // FIXME: return Result
        let bytes = hex::decode(&s).unwrap();
        Self(*GenericArray::from_slice(&bytes))
    }

    pub fn to_string(&self) -> String {
        base64::encode(&self.0[..])
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
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
        Ok(Self(*GenericArray::from_slice(&data[0..64])))
    }
}
