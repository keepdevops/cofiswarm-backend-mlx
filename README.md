# cofiswarm-backend-mlx

Cofiswarm component: `backend-mlx`.

- Layout: [REPO-STANDARD-LAYOUT](https://github.com/keepdevops/cofiswarm-docs/blob/main/REPO-STANDARD-LAYOUT.md)
- Migration: [MIGRATION-SPRINTS](https://github.com/keepdevops/cofiswarm-docs/blob/main/MIGRATION-SPRINTS.md)

## FHS paths

| Path | Purpose |
|------|---------|
| `/etc/cofiswarm/backend-mlx/` | config |
| `/var/lib/cofiswarm/backend-mlx/` | state |
| `/var/log/cofiswarm/backend-mlx/` | logs |

## Test

```bash
./test/scripts/assert-layout.sh backend-mlx
```
