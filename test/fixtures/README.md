# Test fixtures

Generated with the official NATS tooling, so tests check this library against
independent output rather than against itself. They are test-only
credentials for throwaway servers.

- `nkey-user.json`: a user NKey from `nk -gen user -pubout` (nkeys 0.4.17),
  and the signature of `nonce` from `nk -sign`.
- `operator.conf`: operator JWT plus memory resolver with the account JWT,
  from `nsc generate config --mem-resolver` (nsc 2.15.0).
- `user.creds`, `limited.creds`: from `nsc generate creds` for users of that
  account. `LimitedUser` was created with `--deny-pub "forbidden.>"`.

Operator and account signing keys are not included.
