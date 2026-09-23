# FCDS fixed groups

Branch: codex/fcds-fixed-groups. Existing allocation functions are unchanged.
The overlay sets minimum/target/maximum 1/3/3, a 60-second smaller-group wait,
and maximum age zero for adding new members to existing rooms. Full trios start
immediately. Remaining users form a group when its oldest member has waited 60
seconds. Assigned identities use the original reconnect path.

Optional LOBBY_* environment variables preserve old defaults for other installs.
The Dev overlay persists the module/endpoints and a room counter on Bree. Reserve
numbers before scheduling; never run multiple assigners sharing this counter.

Build using deploy/Dockerfile.fcds with BASE_IMAGE set to a locally tagged copy
of the current Lobby image. This preserves dependencies without downloads.
Recreate only Compose service web with the existing project/base file and this
overlay, using --no-deps --no-build. Back up the previous image and configuration.

The inherited Lobby resets its assignment database at startup: drain waiting
and active users before recreation. This patch preserves that lifecycle.
Reconnection within a running service is tested separately.

Run python3 -m unittest discover -s tests -v. These tests execute the production
assignment functions with a controlled clock and rooms, avoiding import-time DB
initialization. Live tests must use getJupyterlabUrl, not preassigned rooms.
