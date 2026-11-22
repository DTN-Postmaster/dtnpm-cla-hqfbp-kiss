//! Configuration types for the HQFBP Convergence Layer Adapter.

use std::net::SocketAddr;
use std::string::String;
use std::time::Duration;
use std::vec::Vec;

use hardy_bpv7::eid::NodeId;

use crate::encoding::ContentEncoding;

#[allow(dead_code)]
fn default_tcp_port() -> u8 {
    0
}

#[allow(dead_code)]
fn default_baud_rate() -> u32 {
    9600
}

#[allow(dead_code)]
const fn default_burst_capacity() -> usize {
    2048
}

#[allow(dead_code)]
fn default_hailing_encodings() -> Vec<ContentEncoding> {
    vec![ContentEncoding::H, ContentEncoding::Crc32]
}

#[allow(dead_code)]
fn default_bundle_encodings() -> Vec<ContentEncoding> {
    vec![
        ContentEncoding::Chunk(240),
        ContentEncoding::H,
        ContentEncoding::Crc32,
    ]
}

#[allow(dead_code)]
const fn default_max_senders() -> usize {
    64
}

#[allow(dead_code)]
const fn default_max_in_flight_bytes() -> usize {
    16 * 1024 * 1024 // 16 MB
}

#[allow(dead_code)]
const fn default_session_ttl() -> Duration {
    Duration::from_secs(60)
}

#[allow(dead_code)]
const fn default_sweep_interval() -> Duration {
    Duration::from_secs(5)
}

#[allow(dead_code)]
const fn default_reconnect_initial() -> Duration {
    Duration::from_millis(500)
}

#[allow(dead_code)]
const fn default_reconnect_max() -> Duration {
    Duration::from_secs(30)
}

/// The underlying link transport utilized by the HQFBP CLA.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "type", rename_all = "kebab-case"))]
pub enum TransportMode {
    /// KISS over TCP framing for packet radio / TNC (e.g. Direwolf, soundmodem).
    #[cfg_attr(
        feature = "serde",
        serde(rename = "kiss-tcp", rename_all = "kebab-case")
    )]
    KissTcp {
        /// Remote KISS TCP server socket address (e.g. `127.0.0.1:8001`).
        server_addr: SocketAddr,
        /// Optional local address to bind when connecting out.
        #[cfg_attr(feature = "serde", serde(default))]
        bind_addr: Option<SocketAddr>,
        /// KISS port nibble (0..15, default 0).
        #[cfg_attr(feature = "serde", serde(default = "default_tcp_port"))]
        port: u8,
        /// TxDelay in 10ms units (KISS Command 1).
        #[cfg_attr(feature = "serde", serde(default))]
        tx_delay: Option<u8>,
        /// P-persistence value (KISS Command 2).
        #[cfg_attr(feature = "serde", serde(default))]
        persistence: Option<u8>,
        /// SlotTime in 10ms units (KISS Command 3).
        #[cfg_attr(feature = "serde", serde(default))]
        slot_time: Option<u8>,
        /// TxTail in 10ms units (KISS Command 4).
        #[cfg_attr(feature = "serde", serde(default))]
        tx_tail: Option<u8>,
        /// Initial reconnect delay on connection loss (default 500ms).
        #[cfg_attr(
            feature = "serde",
            serde(
                default = "default_reconnect_initial",
                with = "dtnpm_core::duration::serde_duration"
            )
        )]
        reconnect_initial: Duration,
        /// Maximum reconnect backoff delay (default 30s).
        #[cfg_attr(
            feature = "serde",
            serde(
                default = "default_reconnect_max",
                with = "dtnpm_core::duration::serde_duration"
            )
        )]
        reconnect_max: Duration,
        /// Optional rate limit in bytes per second for the aggregate KISS stream (e.g. 1200 or 9600).
        #[cfg_attr(feature = "serde", serde(default))]
        rate_limit_bytes_per_sec: Option<u64>,
        /// Maximum burst capacity in bytes for the leaky bucket. Defaults to 2048 bytes if rate limiting is enabled.
        #[cfg_attr(feature = "serde", serde(default = "default_burst_capacity"))]
        burst_capacity_bytes: usize,
    },
    /// KISS over asynchronous serial/TTY port (e.g. `/dev/ttyUSB0`).
    #[cfg_attr(
        feature = "serde",
        serde(rename = "kiss-serial", rename_all = "kebab-case")
    )]
    KissSerial {
        /// Path to the serial device file (e.g. `"/dev/ttyUSB0"` or `"COM3"`).
        path: String,
        /// Baud rate for serial communication (default 9600).
        #[cfg_attr(feature = "serde", serde(default = "default_baud_rate"))]
        baud_rate: u32,
        /// KISS port nibble (0..15, default 0).
        #[cfg_attr(feature = "serde", serde(default = "default_tcp_port"))]
        port: u8,
        /// TxDelay in 10ms units (KISS Command 1).
        #[cfg_attr(feature = "serde", serde(default))]
        tx_delay: Option<u8>,
        /// P-persistence value (KISS Command 2).
        #[cfg_attr(feature = "serde", serde(default))]
        persistence: Option<u8>,
        /// SlotTime in 10ms units (KISS Command 3).
        #[cfg_attr(feature = "serde", serde(default))]
        slot_time: Option<u8>,
        /// TxTail in 10ms units (KISS Command 4).
        #[cfg_attr(feature = "serde", serde(default))]
        tx_tail: Option<u8>,
        /// Optional rate limit in bytes per second for the aggregate KISS stream (e.g. 1200 or 9600).
        #[cfg_attr(feature = "serde", serde(default))]
        rate_limit_bytes_per_sec: Option<u64>,
        /// Maximum burst capacity in bytes for the leaky bucket. Defaults to 2048 bytes if rate limiting is enabled.
        #[cfg_attr(feature = "serde", serde(default = "default_burst_capacity"))]
        burst_capacity_bytes: usize,
    },
    /// Direct UDP datagram transport (e.g. for GNURadio flowgraphs or raw packet broadcast).
    #[cfg_attr(feature = "serde", serde(rename = "udp", rename_all = "kebab-case"))]
    Udp {
        /// Local socket address to bind and listen on.
        bind_addr: SocketAddr,
        /// Remote destination socket address for outgoing broadcasts or peer transmission.
        #[cfg_attr(feature = "serde", serde(default))]
        peer_addr: Option<SocketAddr>,
    },
}

impl TransportMode {
    /// Configures the leaky-bucket rate limiter.
    pub fn with_rate_limit(mut self, rate_bytes_per_sec: u64, burst_capacity: usize) -> Self {
        match self {
            Self::KissTcp {
                ref mut rate_limit_bytes_per_sec,
                ref mut burst_capacity_bytes,
                ..
            } => {
                *rate_limit_bytes_per_sec = Some(rate_bytes_per_sec);
                *burst_capacity_bytes = burst_capacity;
            }
            Self::KissSerial {
                ref mut rate_limit_bytes_per_sec,
                ref mut burst_capacity_bytes,
                ..
            } => {
                *rate_limit_bytes_per_sec = Some(rate_bytes_per_sec);
                *burst_capacity_bytes = burst_capacity;
            }
            _ => {}
        }
        self
    }
}

/// A configured remote peer mapping.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case"))]
pub struct PeerConfig {
    /// Remote amateur radio callsign (e.g. `"F4JXQ"`).
    pub callsign: String,
    /// Associated BPA Node ID.
    #[cfg_attr(feature = "serde", serde(default))]
    pub node_id: Option<NodeId>,
    /// Optional remote network address for IP-based transports.
    #[cfg_attr(feature = "serde", serde(default))]
    pub address: Option<SocketAddr>,
    /// Optional peer-specific encoding pipeline override.
    #[cfg_attr(
        feature = "serde",
        serde(default, with = "crate::encoding::serde_option_encodings")
    )]
    pub encodings: Option<Vec<ContentEncoding>>,
}

/// Top-level configuration for the HQFBP Convergence Layer Adapter.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case"))]
pub struct Config {
    /// Local amateur radio callsign (e.g. `"N0CALL"`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub callsign: Option<String>,

    /// Require a valid, identified amateur radio callsign before starting RF transmission.
    #[cfg_attr(feature = "serde", serde(default))]
    pub require_callsign: bool,

    /// Underlying physical or network link transport.
    pub transport: TransportMode,

    /// Pipeline of content encodings for the hailing/announcement channel.
    #[cfg_attr(
        feature = "serde",
        serde(
            default = "default_hailing_encodings",
            with = "crate::encoding::serde_encodings"
        )
    )]
    pub hailing_encodings: Vec<ContentEncoding>,

    /// Default pipeline of content encodings applied to bundle payloads.
    #[cfg_attr(
        feature = "serde",
        serde(
            default = "default_bundle_encodings",
            with = "crate::encoding::serde_encodings"
        )
    )]
    pub default_bundle_encodings: Vec<ContentEncoding>,

    /// Initial message ID counter for HQFBP generation.
    #[cfg_attr(feature = "serde", serde(default = "default_msg_id"))]
    pub initial_msg_id: u32,

    /// Pre-configured peer entries.
    #[cfg_attr(feature = "serde", serde(default))]
    pub peers: Vec<PeerConfig>,

    /// Maximum number of concurrent sending stations tracked simultaneously.
    #[cfg_attr(feature = "serde", serde(default = "default_max_senders"))]
    pub max_concurrent_senders: usize,

    /// Maximum in-flight buffer memory in bytes allocated across all active sessions.
    #[cfg_attr(feature = "serde", serde(default = "default_max_in_flight_bytes"))]
    pub max_in_flight_bytes: usize,

    /// Inactivity timeout before purging an uncompleted reassembly session.
    #[cfg_attr(
        feature = "serde",
        serde(
            default = "default_session_ttl",
            with = "dtnpm_core::duration::serde_duration"
        )
    )]
    pub session_ttl: Duration,

    /// Interval at which expired sessions and inactive senders are swept.
    #[cfg_attr(
        feature = "serde",
        serde(
            default = "default_sweep_interval",
            with = "dtnpm_core::duration::serde_duration"
        )
    )]
    pub sweep_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            callsign: None,
            require_callsign: false,
            transport: TransportMode::Udp {
                bind_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 10042)),
                peer_addr: None,
            },
            hailing_encodings: default_hailing_encodings(),
            default_bundle_encodings: default_bundle_encodings(),
            initial_msg_id: default_msg_id(),
            peers: Vec::new(),
            max_concurrent_senders: default_max_senders(),
            max_in_flight_bytes: default_max_in_flight_bytes(),
            session_ttl: default_session_ttl(),
            sweep_interval: default_sweep_interval(),
        }
    }
}

/// Returns `true` if `callsign` is missing, blank, or placeholder `N0CALL` (with optional SSID).
pub fn is_unidentified_callsign(callsign: Option<&str>) -> bool {
    match callsign {
        None => true,
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return true;
            }
            let base = trimmed.split('-').next().unwrap_or(trimmed);
            base.is_empty() || base.eq_ignore_ascii_case("N0CALL")
        }
    }
}

impl Config {
    /// Validate local RF callsign configuration according to [`Self::require_callsign`].
    ///
    /// When `callsign` is absent or `N0CALL`, emits a warning by default so local tests and
    /// receive-only listeners work out of the box, or returns [`crate::Error::InvalidCallsign`]
    /// when `require_callsign` is enabled.
    pub fn validate_callsign(&self) -> crate::Result<()> {
        if is_unidentified_callsign(self.callsign.as_deref()) {
            if self.require_callsign {
                return Err(crate::Error::InvalidCallsign(
                    "identified amateur radio callsign is required (got None or N0CALL); set `callsign` or pass `--allow-unidentified-rf`".to_string(),
                ));
            }
            tracing::warn!(
                callsign = ?self.callsign,
                "HQFBP CLA running with unidentified RF callsign (None or N0CALL); set `callsign` in config or pass `--require-callsign` before on-air transmission"
            );
        }
        Ok(())
    }
}

#[allow(dead_code)]
const fn default_msg_id() -> u32 {
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let transport = TransportMode::Udp {
            bind_addr: "127.0.0.1:10042".parse().unwrap(),
            peer_addr: None,
        };
        let cfg = Config {
            callsign: Some("F4JXQ".to_string()),
            require_callsign: false,
            transport,
            hailing_encodings: default_hailing_encodings(),
            default_bundle_encodings: default_bundle_encodings(),
            initial_msg_id: default_msg_id(),
            peers: Vec::new(),
            max_concurrent_senders: default_max_senders(),
            max_in_flight_bytes: default_max_in_flight_bytes(),
            session_ttl: default_session_ttl(),
            sweep_interval: default_sweep_interval(),
        };

        assert_eq!(default_tcp_port(), 0);
        assert_eq!(default_baud_rate(), 9600);
        assert_eq!(default_msg_id(), 1);
        assert_eq!(default_max_in_flight_bytes(), 16 * 1024 * 1024);
        assert_eq!(cfg.max_concurrent_senders, 64);
        assert_eq!(cfg.session_ttl, Duration::from_secs(60));
        assert_eq!(cfg.max_in_flight_bytes, 16 * 1024 * 1024);
        assert_eq!(
            cfg.hailing_encodings,
            vec![ContentEncoding::H, ContentEncoding::Crc32]
        );
        assert_eq!(
            cfg.default_bundle_encodings,
            vec![
                ContentEncoding::Chunk(240),
                ContentEncoding::H,
                ContentEncoding::Crc32
            ]
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_defaults() {
        let json = r#"{
            "transport": {
                "type": "kiss-tcp",
                "server-addr": "127.0.0.1:8001"
            }
        }"#;
        let cfg: Config = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.initial_msg_id, 1);
        assert_eq!(cfg.max_in_flight_bytes, 16 * 1024 * 1024);
        assert_eq!(cfg.session_ttl, Duration::from_secs(60));
        assert_eq!(
            cfg.hailing_encodings,
            vec![ContentEncoding::H, ContentEncoding::Crc32]
        );
        assert_eq!(
            cfg.default_bundle_encodings,
            vec![
                ContentEncoding::Chunk(240),
                ContentEncoding::H,
                ContentEncoding::Crc32
            ]
        );
        if let TransportMode::KissTcp {
            port,
            reconnect_initial,
            reconnect_max,
            ..
        } = cfg.transport
        {
            assert_eq!(port, 0);
            assert_eq!(reconnect_initial, Duration::from_millis(500));
            assert_eq!(reconnect_max, Duration::from_secs(30));
        } else {
            panic!("expected KissTcp");
        }

        let serial_json = r#"{
            "transport": {
                "type": "kiss-serial",
                "path": "/dev/ttyUSB0"
            },
            "session-ttl": "120s",
            "sweep-interval": "10s"
        }"#;
        let serial_cfg: Config = serde_json::from_str(serial_json).unwrap();
        assert_eq!(serial_cfg.session_ttl, Duration::from_secs(120));
        assert_eq!(serial_cfg.sweep_interval, Duration::from_secs(10));
        if let TransportMode::KissSerial {
            baud_rate, port, ..
        } = serial_cfg.transport
        {
            assert_eq!(baud_rate, 9600);
            assert_eq!(port, 0);
        } else {
            panic!("expected KissSerial");
        }

        let paced_json = r#"{
            "transport": {
                "type": "kiss-tcp",
                "server-addr": "127.0.0.1:8001",
                "rate-limit-bytes-per-sec": 1200,
                "burst-capacity-bytes": 4096
            }
        }"#;
        let paced_cfg: Config = serde_json::from_str(paced_json).unwrap();
        if let TransportMode::KissTcp {
            rate_limit_bytes_per_sec,
            burst_capacity_bytes,
            ..
        } = paced_cfg.transport
        {
            assert_eq!(rate_limit_bytes_per_sec, Some(1200));
            assert_eq!(burst_capacity_bytes, 4096);
        } else {
            panic!("expected KissTcp with rate limiting");
        }

        let serial_paced_json = r#"{
            "transport": {
                "type": "kiss-serial",
                "path": "/dev/ttyUSB1",
                "rate-limit-bytes-per-sec": 300
            }
        }"#;
        let serial_paced: Config = serde_json::from_str(serial_paced_json).unwrap();
        if let TransportMode::KissSerial {
            rate_limit_bytes_per_sec,
            burst_capacity_bytes,
            ..
        } = serial_paced.transport
        {
            assert_eq!(rate_limit_bytes_per_sec, Some(300));
            assert_eq!(burst_capacity_bytes, 2048); // default
        } else {
            panic!("expected KissSerial with rate limiting");
        }

        let fluent = TransportMode::Udp {
            bind_addr: "127.0.0.1:10042".parse().unwrap(),
            peer_addr: None,
        }
        .with_rate_limit(500, 1000);
        assert!(matches!(fluent, TransportMode::Udp { .. }));

        let fluent_tcp = TransportMode::KissTcp {
            server_addr: "127.0.0.1:8001".parse().unwrap(),
            bind_addr: None,
            port: 0,
            tx_delay: None,
            persistence: None,
            slot_time: None,
            tx_tail: None,
            reconnect_initial: Duration::from_millis(500),
            reconnect_max: Duration::from_secs(30),
            rate_limit_bytes_per_sec: None,
            burst_capacity_bytes: 2048,
        }
        .with_rate_limit(1200, 4096);

        if let TransportMode::KissTcp {
            rate_limit_bytes_per_sec,
            burst_capacity_bytes,
            ..
        } = fluent_tcp
        {
            assert_eq!(rate_limit_bytes_per_sec, Some(1200));
            assert_eq!(burst_capacity_bytes, 4096);
        } else {
            panic!("expected KissTcp");
        }
    }

    #[test]
    fn test_callsign_identification_policy() {
        assert!(is_unidentified_callsign(None));
        assert!(is_unidentified_callsign(Some("")));
        assert!(is_unidentified_callsign(Some("   ")));
        assert!(is_unidentified_callsign(Some("N0CALL")));
        assert!(is_unidentified_callsign(Some("n0call-1")));
        assert!(!is_unidentified_callsign(Some("F4JXQ")));
        assert!(!is_unidentified_callsign(Some("F4JXQ-7")));

        let mut cfg = Config::default();
        assert!(!cfg.require_callsign);
        assert!(cfg.validate_callsign().is_ok());

        cfg.callsign = Some("N0CALL".to_string());
        assert!(cfg.validate_callsign().is_ok());

        cfg.require_callsign = true;
        assert!(matches!(
            cfg.validate_callsign(),
            Err(crate::Error::InvalidCallsign(_))
        ));

        cfg.callsign = None;
        assert!(matches!(
            cfg.validate_callsign(),
            Err(crate::Error::InvalidCallsign(_))
        ));

        cfg.callsign = Some("F4JXQ".to_string());
        assert!(cfg.validate_callsign().is_ok());
    }
}
