# Service diagnostics summary

Source: the container log dump of the 10 October 2026 acceptance run (400 lines).
This note records what that dump contained.

## Lines per service

| Service | Lines |
|---|---:|
| `mongodb-1` | 80 |
| `datanode-1` | 80 |
| `spark-master-1` | 80 |
| `namenode-1` | 80 |
| `spark-worker-1` | 80 |

## Notable lines

- 26/10/10 03:12:39 WARN Master: Got status update for unknown executor app-20261010031205-0000/0
- 26/10/10 03:39:13 WARN Master: Got status update for unknown executor app-20261010031528-0001/1
- 26/10/10 03:47:41 WARN Master: Got status update for unknown executor app-20261010034340-0002/0
- 26/10/10 03:50:57 WARN Master: Got status update for unknown executor app-20261010035031-0003/0
- 26/10/10 04:36:44 WARN Master: Got status update for unknown executor app-20261010035347-0004/0
- 26/10/10 05:21:40 WARN Master: Got status update for unknown executor app-20261010044026-0005/0
