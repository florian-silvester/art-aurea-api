#!/bin/bash
# Sync script from aa_scan to art-aurea-api
# Run this before deploying to keep sync script in sync

AA_SCAN_PATH="../aa_scan"
SYNC_SCRIPT="api/sync-to-webflow.js"

if [ ! -f "$AA_SCAN_PATH/$SYNC_SCRIPT" ]; then
  echo "❌ Error: Cannot find $AA_SCAN_PATH/$SYNC_SCRIPT"
  echo "   Make sure aa_scan is in the parent directory"
  exit 1
fi

# Remove symlink if it exists
if [ -L "$SYNC_SCRIPT" ]; then
  echo "🔗 Removing symlink..."
  rm "$SYNC_SCRIPT"
fi

# Copy the file
echo "📋 Copying sync script from aa_scan..."
cp "$AA_SCAN_PATH/$SYNC_SCRIPT" "$SYNC_SCRIPT"

# Check if git sees it as changed
if git diff --quiet "$SYNC_SCRIPT"; then
  echo "✅ Sync script is already up to date"
else
  echo "✅ Sync script copied and ready to commit"
  echo "   Run: git add $SYNC_SCRIPT && git commit -m 'Update sync script from aa_scan'"
fi

