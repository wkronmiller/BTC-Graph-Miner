# Stage 1

1.Load a transaction
2. For each input ID in a transaction, search dht for input-to-group mapping
3. For all input IDs with group assignments, pick a group at random, assign all found groups and input transaction groups to a shared group (picked at random)

# Stage 2

1. Iterate over all groups, for each group iterate over all output id's (note each output ID should correspond to a group ID)
2. Build a map counting references between groups
3. Do ai random walk per original write-up where we start in some random groups, and probabilistically assign them to a single group based on a walk of the graph
