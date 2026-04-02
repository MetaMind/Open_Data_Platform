# MetaMind Documentation

Welcome to the MetaMind Enterprise Query Intelligence Platform documentation.

## Getting Started

New to MetaMind? Start here:

- **[Quick Start](../README.md#quick-start)** - Get up and running in minutes
- **[Architecture](architecture.md)** - Understand the system design
- **[Configuration](configuration.md)** - Configure MetaMind for your environment

## API Reference

- **[API Documentation](API.md)** - Complete API reference with examples
- **[Query Routing](API.md#query-routing-logic)** - Understand routing decisions
- **[Error Handling](API.md#error-handling)** - Handle API errors

## Deployment

- **[Deployment Guide](deployment.md)** - Deploy to production
- **[Configuration](configuration.md)** - Environment-specific settings
- **[Monitoring](monitoring.md)** - Set up observability

## Development

- **[Development Guide](development.md)** - Contribute to MetaMind
- **[Testing](development.md#testing)** - Run and write tests
- **[Architecture](architecture.md)** - System design details

## Operations

- **[Monitoring](monitoring.md)** - Metrics, logs, and alerts
- **[Troubleshooting](troubleshooting.md)** - Common issues and solutions
- **[Performance Tuning](monitoring.md#performance-monitoring)** - Optimize performance

## Key Concepts

### Query Routing

MetaMind routes queries to the optimal engine based on:
- **Freshness Requirements** - Real-time vs. historical data
- **ML Cost Model** - Predicted execution cost
- **Engine Health** - Circuit breaker state
- **Query Complexity** - Batch job detection

### Execution Engines

| Engine | Use Case | Best For |
|--------|----------|----------|
| Oracle | OLTP | Real-time queries |
| Trino/S3 | OLAP | Interactive analytics |
| Spark | Batch | Large-scale processing |

### CDC Pipeline

```
Oracle → Debezium → Kafka → Spark → Iceberg (S3)
```

CDC lag determines routing decisions for freshness-sensitive queries.

## Documentation Structure

```
docs/
├── index.md              # This file
├── architecture.md       # System architecture
├── API.md               # API reference
├── configuration.md     # Configuration guide
├── deployment.md        # Deployment guide
├── monitoring.md        # Observability guide
├── development.md       # Development guide
└── troubleshooting.md   # Troubleshooting guide
```

## Quick Links

- [GitHub Repository](https://github.com/metamind/metamind)
- [Issue Tracker](https://github.com/metamind/metamind/issues)
- [Changelog](../CHANGELOG.md)
- [License](../LICENSE)

## Support

- **Documentation**: https://docs.metamind.io
- **Email**: support@metamind.io
- **Slack**: [Join our community](https://metamind.slack.com)

## Contributing

We welcome contributions! See the [Development Guide](development.md) for details.

---

*Last updated: March 2024*
