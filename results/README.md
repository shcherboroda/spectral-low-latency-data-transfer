# Published benchmark summaries

These CSV files are the compact source data for `analysis.ipynb`. They retain
every observation used by the notebook, alongside the exact run identifiers,
but omit verbose component logs and per-message latency CSVs.

The measurements were collected on a WSL development laptop using isolated
Linux network namespaces and `tc netem`; they are comparative evidence, not
claims about real-network or target-cloud latency. See `SOLUTION.md` for the
commands, methodology, and limitations.

- `tail.csv`: three zero-loss repetitions for packed and unpacked XOR FEC.
- `recovery.csv`: three repetitions per FEC profile and loss model.
- `fanout_loss.csv`: three-receiver loss experiments.
