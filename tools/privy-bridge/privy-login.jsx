// Motion ⇄ Privy bridge, connect-only edition.
// Privy just connects the wallet (its picker modal, no sign-in signature of its own);
// the site then asks for exactly one personal_sign to open a session.
// Window API kept identical to the previous bundle:
//   window.PrivyMount(appId, opts)     mount once, fires 'privy:ready'
//   window.PrivyLogin.open()           open the wallet picker
//   window.PrivyLogin.logout()         drop connected wallets
//   events on document: privy:ready, privy:wallet {address, provider, name}, privy:error
import React, { useEffect, useRef } from 'react';
import { createRoot } from 'react-dom/client';
import { PrivyProvider, usePrivy, useWallets, useConnectWallet } from '@privy-io/react-auth';

function Bridge() {
  const { ready } = usePrivy();
  const { wallets } = useWallets();
  const seen = useRef(new Set());
  const { connectWallet } = useConnectWallet({
    onError: (err) => document.dispatchEvent(new CustomEvent('privy:error', { detail: String(err && err.message || err) })),
  });

  useEffect(() => {
    window.PrivyLogin = {
      open: () => connectWallet(),
      logout: async () => { for (const w of wallets) { try { await w.disconnect?.(); } catch {} } seen.current.clear(); },
      ready,
    };
    if (ready) document.dispatchEvent(new CustomEvent('privy:ready'));
  }, [ready, wallets, connectWallet]);

  useEffect(() => {
    if (!ready || !wallets.length) return;
    const w = wallets[wallets.length - 1];                 // the one just connected
    if (!w?.address || seen.current.has(w.address.toLowerCase())) return;
    seen.current.add(w.address.toLowerCase());
    w.getEthereumProvider().then((provider) => {
      document.dispatchEvent(new CustomEvent('privy:wallet', { detail: { address: w.address.toLowerCase(), provider, name: w.walletClientType || 'wallet' } }));
    }).catch((err) => document.dispatchEvent(new CustomEvent('privy:error', { detail: String(err && err.message || err) })));
  }, [ready, wallets.length]);

  return null;
}

window.PrivyMount = (appId, opts = {}) => {
  const host = document.createElement('div'); host.id = 'privy-root'; document.body.append(host);
  const chain = opts.chain;
  createRoot(host).render(
    <PrivyProvider
      appId={appId}
      config={{
        loginMethods: ['wallet'],
        appearance: {
          theme: opts.theme || '#161616',
          accentColor: opts.accent || '#ffffff',
          logo: opts.logo,
          landingHeader: opts.header || 'Connect wallet',
          loginMessage: opts.message || 'Connect the wallet that launched your coin.',
          walletList: ['metamask', 'rabby_wallet', 'coinbase_wallet', 'wallet_connect', 'detected_wallets'],
          walletChainType: 'ethereum-only',
          showWalletLoginFirst: true,
        },
        embeddedWallets: { ethereum: { createOnLogin: 'off' } },
        ...(chain ? { supportedChains: [chain], defaultChain: chain } : {}),
      }}
    >
      <Bridge />
    </PrivyProvider>
  );
};
