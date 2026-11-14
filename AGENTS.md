# Agent Guide — `dtnpm-cla-hqfbp-kiss`

## 🎯 Mission
Fournir l'adaptateur de couche de convergence (CLA) pour le protocole de diffusion radioamateur rapide HQFBP (Hamradio Quick File Broadcasting Protocol) sur canaux radioamateurs (KISS TCP pour Direwolf/Soundmodem, KISS Série/TTY, et UDP pour SDR), avec support de la correction d'erreur directe (FEC Reed-Solomon et RaptorQ).

---

## 🛡️ Référentiels et Exigences de Sécurité

### 1. Cyber Resilience Act (CRA) & Sécurité Radioamateur
- **Validation d'Identité Radioamateur** :
  - Les indicatifs d'appel sont normalisés et validés avec rigueur syntaxique (`is_valid_callsign` : 3 à 10 caractères alphanumériques ASCII, au moins une lettre, SSID optionnel `-0` à `-15`, rejet strict de `\r`, `\n`, `\0`).
- **Opt-in Explicite pour les Tests en Laboratoire** :
  - L'exploitation avec un indicatif non identifié (blanc ou `N0CALL`) émet un avertissement par défaut et n'est autorisée en transmission que sur demande explicite via `--allow-unidentified-rf` ou `require_callsign: false`.
  - Le drapeau `--require-callsign` permet d'imposer un indicatif strictement valide avant toute transmission RF.
- **Supply-Chain & VEX** :
  - Dépendances auditées avec `cargo-audit` et `cargo-deny` ; déclaration VEX `not_affected` pour `RUSTSEC-2026-0459`.
  - Utilisation de `yaml_serde` (sans libyaml).
  - Actions GitHub CI épinglées par SHA-1 à 40 caractères.
- **Politique de sécurité** : Consignée dans [SECURITY.md](SECURITY.md).

### 2. Robustesse Protocolaire & Isolation des Sessions
- **Protection DoS Inter-Stations (`RxSessionManager`)** :
  - L'éviction d'un émetteur ancien (`evict_oldest_sender`) passe le TTL configuré (`self.session_ttl`) et ne doit JAMAIS réinitialiser à zéro les sessions actives des autres stations radio.
  - Nettoyage préventif des sessions expirées (`sweep_expired`) avant tout délestage de capacité ou LRU.
- **Comptabilité Mémoire Résiliente au Bruit Radio** :
  - Comptabilisation par chunk (`chunk_bytes: HashMap<(u32, u32), usize>`) sur `SenderState` avec déduction des doublons pour éviter toute fuite de quota mémoire lors de trames dupliquées ou bruitées.
  - Plafond par émetteur imposé (`per_sender_byte_cap = max_in_flight_bytes / 2`).
- **Échappement Protocolaire KISS** :
  - Échappement systématique des octets de contrôle `FEND` (`0xC0`) et `FESC` (`0xDB`) dans `make_kiss_param_frame`.
- **Résilience Réseau** :
  - Reconnexion automatique avec backoff exponentiel (`reconnect_initial` à `reconnect_max`) en cas d'échec de liaison TCP avec le TNC.

---

## 🦀 Standards d'Ingénierie Rust

1. **Règle Zéro Panic / Zéro Unwrap** :
   - Tout le code sous `src/` doit être 100% exempt de `.unwrap()`, `.expect()` et de macro `panic!`.
   - Linter strict : `-D warnings -D clippy::unwrap_used -D clippy::panic`.
2. **ADRs Obligatoires** :
   - Toute décision architecturale HQFBP/KISS doit être consignée sous `adr/` et indexée dans [adr/README.md](adr/README.md).

---

## ⚡ Commandes Critiques de Vérification

- `cargo fmt --all -- --check`
- `cargo clippy --offline --workspace --lib --bins -- -D warnings -D clippy::unwrap_used -D clippy::panic`
- `cargo test --offline --all-features`
- `./scripts/check-security.sh --strict`
- `./scripts/check-coverage.sh --summary-only`
