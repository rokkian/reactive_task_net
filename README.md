# Reactive Task Net

This architecture is viable to implement comunication based contract net, where a Master who owns a task decides on parameters who is the best worker to execute the task between the ones that proposed.

Here the task Master is implemented in Python3 and the workers are implemented in Java Maven.

There can be multiple task master and workers active at the same moment and if MAX_NETWORK_DELAY and MAX_EXECUTION_TIME are known the system is process crash tolerant.

This demo can be the base of systems like auctions or other real-time assegantion problems.

## Requirements

It's required to have an accesible Redis instance either local or remote.

For the worker agents it's required Java and Maven.

For Python it's required at least version 3.8.
