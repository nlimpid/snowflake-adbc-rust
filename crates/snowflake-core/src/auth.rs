//! Authenticators. Password, key-pair JWT, programmatic access token, OAuth.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey, LineEnding};
use rsa::{Pkcs1v15Sign, RsaPrivateKey, RsaPublicKey};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// A string that does not show up in `Debug`.
#[derive(Clone)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretString(***)")
    }
}

/// How to obtain a Snowflake session.
#[derive(Clone)]
pub enum Authenticator {
    Password {
        password: SecretString,
    },
    KeyPair {
        private_key: Box<RsaPrivateKey>,
        public_key_fingerprint: String,
    },
    ProgrammaticAccessToken {
        token: SecretString,
    },
    OAuth {
        token: SecretString,
    },
}

impl std::fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Password { .. } => f.write_str("Password(***)"),
            Self::KeyPair {
                public_key_fingerprint,
                ..
            } => f
                .debug_struct("KeyPair")
                .field("public_key_fingerprint", public_key_fingerprint)
                .finish_non_exhaustive(),
            Self::ProgrammaticAccessToken { .. } => f.write_str("ProgrammaticAccessToken(***)"),
            Self::OAuth { .. } => f.write_str("OAuth(***)"),
        }
    }
}

/// Fields placed on the login request for one attempt.
pub struct LoginMaterial {
    pub authenticator: &'static str,
    pub password: Option<String>,
    pub token: Option<String>,
}

impl Authenticator {
    pub fn password(password: impl Into<String>) -> Self {
        Self::Password {
            password: SecretString::new(password),
        }
    }

    pub fn programmatic_access_token(token: impl Into<String>) -> Self {
        Self::ProgrammaticAccessToken {
            token: SecretString::new(token),
        }
    }

    pub fn oauth(token: impl Into<String>) -> Self {
        Self::OAuth {
            token: SecretString::new(token),
        }
    }

    pub fn key_pair_from_pem(pem: &str, passphrase: Option<&str>) -> Result<Self> {
        let private_key = load_private_key(pem, passphrase)?;
        let public_key_fingerprint = public_key_fingerprint(&private_key)?;
        Ok(Self::KeyPair {
            private_key: Box::new(private_key),
            public_key_fingerprint,
        })
    }

    pub fn key_pair_from_path(
        path: impl AsRef<std::path::Path>,
        passphrase: Option<&str>,
    ) -> Result<Self> {
        let pem = std::fs::read_to_string(path.as_ref()).map_err(|err| {
            Error::protocol(format!(
                "read private key {}: {err}",
                path.as_ref().display()
            ))
        })?;
        Self::key_pair_from_pem(&pem, passphrase)
    }

    pub fn login_material(&self, account: &str, user: &str) -> Result<LoginMaterial> {
        match self {
            Self::Password { password } => Ok(LoginMaterial {
                authenticator: "SNOWFLAKE",
                password: Some(password.expose().to_string()),
                token: None,
            }),
            Self::KeyPair {
                private_key,
                public_key_fingerprint,
            } => {
                let token = sign_jwt(private_key, account, user, public_key_fingerprint)?;
                Ok(LoginMaterial {
                    authenticator: "SNOWFLAKE_JWT",
                    password: None,
                    token: Some(token),
                })
            }
            Self::ProgrammaticAccessToken { token } => Ok(LoginMaterial {
                authenticator: "PROGRAMMATIC_ACCESS_TOKEN",
                password: None,
                token: Some(token.expose().to_string()),
            }),
            Self::OAuth { token } => Ok(LoginMaterial {
                authenticator: "OAUTH",
                password: None,
                token: Some(token.expose().to_string()),
            }),
        }
    }

    pub fn fingerprint(&self) -> Option<&str> {
        match self {
            Self::KeyPair {
                public_key_fingerprint,
                ..
            } => Some(public_key_fingerprint),
            _ => None,
        }
    }
}

/// Account identifier as Snowflake expects it inside a JWT.
///
/// `xy12345.us-east-1` becomes `XY12345`. `myorg-myacct` stays `MYORG-MYACCT`.
pub fn normalize_jwt_account(account: &str) -> String {
    let account = account.trim();
    let head = account.split('.').next().unwrap_or(account);
    head.to_ascii_uppercase()
}

pub fn normalize_jwt_user(user: &str) -> String {
    user.trim().to_ascii_uppercase()
}

pub fn public_key_fingerprint(key: &RsaPrivateKey) -> Result<String> {
    let public = RsaPublicKey::from(key);
    let der = public
        .to_public_key_der()
        .map_err(|err| Error::protocol(format!("encode public key: {err}")))?;
    let digest = Sha256::digest(der.as_bytes());
    let encoded = base64::engine::general_purpose::STANDARD.encode(digest);
    Ok(format!("SHA256:{encoded}"))
}

fn sign_jwt(key: &RsaPrivateKey, account: &str, user: &str, fingerprint: &str) -> Result<String> {
    let account = normalize_jwt_account(account);
    let user = normalize_jwt_user(user);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| Error::protocol(format!("system clock is before unix epoch: {err}")))?
        .as_secs();
    let header = base64_url(br#"{"alg":"RS256","typ":"JWT"}"#);
    let payload = serde_json::json!({
        "iss": format!("{account}.{user}.{fingerprint}"),
        "sub": format!("{account}.{user}"),
        "iat": now,
        "exp": now + 60,
    });
    let payload = base64_url(payload.to_string().as_bytes());
    let signing_input = format!("{header}.{payload}");
    let digest = Sha256::digest(signing_input.as_bytes());
    let signature = key
        .sign(Pkcs1v15Sign::new::<Sha256>(), &digest)
        .map_err(|err| Error::protocol(format!("sign jwt: {err}")))?;
    Ok(format!("{signing_input}.{}", base64_url(&signature)))
}

fn base64_url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn load_private_key(pem: &str, passphrase: Option<&str>) -> Result<RsaPrivateKey> {
    if let Some(passphrase) = passphrase {
        return RsaPrivateKey::from_pkcs8_encrypted_pem(pem, passphrase.as_bytes())
            .map_err(|err| Error::protocol(format!("decrypt PKCS#8 private key: {err}")));
    }
    if let Ok(key) = RsaPrivateKey::from_pkcs8_pem(pem) {
        return Ok(key);
    }
    RsaPrivateKey::from_pkcs1_pem(pem)
        .map_err(|err| Error::protocol(format!("parse PKCS#8 or PKCS#1 private key: {err}")))
}

/// PEM export used by tests and by callers that want to round-trip a key.
pub fn private_key_to_pkcs8_pem(key: &RsaPrivateKey) -> Result<String> {
    key.to_pkcs8_pem(LineEnding::LF)
        .map(|pem| pem.to_string())
        .map_err(|err| Error::protocol(format!("encode PKCS#8: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    fn test_key() -> RsaPrivateKey {
        let mut rng = rand::thread_rng();
        RsaPrivateKey::new(&mut rng, 2048).expect("generate key")
    }

    #[test]
    fn jwt_account_strips_region_and_uppercases() {
        assert_eq!(normalize_jwt_account("xy12345.us-east-1"), "XY12345");
        assert_eq!(normalize_jwt_account("myorg-myacct"), "MYORG-MYACCT");
        assert_eq!(normalize_jwt_user("svc_user"), "SVC_USER");
    }

    #[test]
    fn password_debug_is_redacted() {
        let auth = Authenticator::password("s3cret");
        let rendered = format!("{auth:?}");
        assert!(!rendered.contains("s3cret"));
    }

    #[test]
    fn jwt_claims_and_fingerprint_are_stable() {
        let key = test_key();
        let pem = private_key_to_pkcs8_pem(&key).unwrap();
        let auth = Authenticator::key_pair_from_pem(&pem, None).unwrap();
        let again = Authenticator::key_pair_from_pem(&pem, None).unwrap();
        assert_eq!(auth.fingerprint(), again.fingerprint());
        let fp = auth.fingerprint().unwrap();
        assert!(fp.starts_with("SHA256:"));

        let material = auth
            .login_material("xy12345.us-east-1", "svc_user")
            .unwrap();
        assert_eq!(material.authenticator, "SNOWFLAKE_JWT");
        let token = material.token.unwrap();
        let mut parts = token.split('.');
        let header = parts.next().unwrap();
        let payload = parts.next().unwrap();
        let signature = parts.next().unwrap();
        assert!(parts.next().is_none());

        let payload_json = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .unwrap();
        let claims: serde_json::Value = serde_json::from_slice(&payload_json).unwrap();
        let iss = claims["iss"].as_str().unwrap();
        assert!(iss.starts_with("XY12345.SVC_USER.SHA256:"));
        assert_eq!(claims["sub"], "XY12345.SVC_USER");
        assert!(claims["exp"].as_u64().unwrap() - claims["iat"].as_u64().unwrap() == 60);

        let signing_input = format!("{header}.{payload}");
        let digest = Sha256::digest(signing_input.as_bytes());
        let sig = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(signature)
            .unwrap();
        let public = RsaPublicKey::from(&key);
        public
            .verify(Pkcs1v15Sign::new::<Sha256>(), &digest, &sig)
            .expect("signature verifies");
    }

    #[test]
    fn encrypted_pkcs8_round_trip() {
        let key = test_key();
        let pem = key
            .to_pkcs8_encrypted_pem(rand::thread_rng(), "phrase".as_bytes(), LineEnding::LF)
            .unwrap();
        let auth = Authenticator::key_pair_from_pem(pem.as_str(), Some("phrase")).unwrap();
        assert!(auth.fingerprint().unwrap().starts_with("SHA256:"));
        let err = Authenticator::key_pair_from_pem(pem.as_str(), Some("nope")).unwrap_err();
        assert!(err.to_string().contains("decrypt"));
    }
}
