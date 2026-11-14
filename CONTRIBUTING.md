# Guide de Contribution — `dtnpm-cla-hqfbp-kiss`

Ce guide présente les règles de développement et les exigences de sécurité pour le CLA HQFBP et KISS de `dtnpm-cla-hqfbp-kiss`.

---

## 1. Principes Fondamentaux

- **Zéro Panic / Zéro Unwrap** : Aucun appel `.unwrap()` ou `.expect()` en code de production.
- **Sécurité et Bornage des Sessions RF** :
  - La gestion de sessions multi-émetteurs doit isoler les stations pour qu'une perturbation sur une liaison n'affecte pas les autres.
  - La mémoire de déframage doit être strictement bornée et libérée lors du remplacement de paquets dupliqués.
- **Traçabilité Architecturale (ADR)** :
  - Tout ajout ou ajustement protocolaire doit faire l'objet d'un fichier ADR sous `adr/` et être indexé dans `adr/README.md`.

---

## 2. Vérification Locale

```bash
cargo fmt --all -- --check
cargo clippy --offline --workspace --lib --bins -- -D warnings -D clippy::unwrap_used -D clippy::panic
cargo test --offline --all-features
./scripts/check-security.sh --strict
```
