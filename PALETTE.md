# Palette

Current accent: **sky** `#8fd3ff` (sky blue, chosen 2026-09-08; the previous mist #c3d4bd is one commit back), applied 2026-09-08.

Rollback to the original PONS lime (`#d4fc50`):

    git checkout palette-lime -- frontend/index.html frontend/assets/pons.css
    git commit -m "palette: back to lime"

The tag `palette-lime` points at the last lime commit. The swap itself is a literal substitution
(see the commit "palette: mist"), so re-applying it is `git revert` of the rollback commit.
