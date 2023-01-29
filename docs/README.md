# Notes

## Shuffle

- Each query stage has input and output partition count
- There is one task per input stage
- Each task can produce multiple output partitions
- Example: 4 input partitions, 4 output partitions (with different partitioning scheme) results in 16 shuffle files (4 x 4) 