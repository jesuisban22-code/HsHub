# HsHub

Moteur de recherche haute performance multi-bases — architecture client/serveur.

## Architecture

| Composant | Rôle |
|---|---|
| `hshub_v5.html` | Frontend public (GitHub Pages) — léger, zéro data local |
| `hshub-server.js` | Serveur data (Codespaces / VPS) — SQLite, 1B+ lignes |
| `hshub-index.js` | Génère `index.sqlite` depuis les CSV |
| `hshub-to-sqlite.js` | Convertit un ancien `index.bin` en `index.sqlite` |

## Démarrage (Codespaces)

```bash
npm install
node hshub-index.js *.csv -o index.bin   # première fois
node hshub-server.js
```

Rendre le port **7331** public dans l'onglet **Ports**, puis entrer l'URL dans le site.

## Site public

**https://jesuisban22-code.github.io/HsHub/**

Cliquer sur 🌐 Serveur, entrer l'URL Codespaces, enregistrer.
