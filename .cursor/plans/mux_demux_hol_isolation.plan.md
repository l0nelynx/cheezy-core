# Revert demux isolation + spread-first

Field regression after isolation/spread: connection failures or ≤2 Mbps
regardless of concurrency/max-connections. Restore client to
`b541cc33` behavior (512KiB pipe + pack-first + blocking backpressure).

Docs updated accordingly.
