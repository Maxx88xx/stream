#!/bin/sh
# Rebuild frontend/assets/privy.js from privy-login.jsx (connect-only Privy bridge).
set -e; cd "$(dirname "$0")"
npm install --silent
npx --yes esbuild privy-login.jsx --bundle --minify --format=iife --jsx=automatic --target=es2020 --define:process.env.NODE_ENV='"production"' --outfile=../../frontend/assets/privy.js
gzip -9 -k -f ../../frontend/assets/privy.js
