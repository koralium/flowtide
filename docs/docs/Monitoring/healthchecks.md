---
sidebar_position: 1
---

# Health checks

In the 'TideflowDotnet.AspNetCore' package, there is built in support for 'Microsoft.Extensions.Diagnostics.HealthChecks'.
If you have added your stream using:

```
builder.Services.AddFlowtideStream(...)
```

You can add health checks with the following command:

```
builder.Services
    .AddHealthChecks()
    .AddFlowtideCheck();
```

## Statuses

This section describes how the different stream statuses maps to the health check status:

| Stream Status     | Health check status           | Description                                                   |
| ----------------- | ----------------------------- | ------------------------------------------------------------- |
| Failing           | Unhealthy                     | Stream has crashed                                            |
| Running           | Running                       | Operational                                                   |
| Starting          | Degraded                      | Starting is only reported when going from stopped -> running  |
| Stopped           | Unhealthy                     | If a stream should be stopped, remove it from health check    |
| Degraded          | Degraded                      | Reported if a operator is degraded, such as slow performance  |

