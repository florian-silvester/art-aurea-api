# Art Aurea API

API endpoints for syncing Sanity CMS to Webflow.

## Development

The sync script (`api/sync-to-webflow.js`) is symlinked from `aa_scan` for local development. Changes to the script in `aa_scan` are immediately available here.

## Deployment

**Before deploying to Vercel**, run:

```bash
npm run sync-script
git add api/sync-to-webflow.js
git commit -m "Update sync script from aa_scan"
git push
```

This copies the latest sync script from `aa_scan` and commits it, ensuring Vercel has the correct version (Vercel can't follow symlinks during builds).

## Workflow

1. **Local development**: Symlink keeps scripts in sync automatically
2. **Before pushing**: Run `npm run sync-script` to copy the file
3. **Commit & push**: The copied file is what gets deployed to Vercel
