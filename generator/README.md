# Generator

The Generator component is used to generate new users and simulate user actions. The generated events will be sent to Kafka.

### How the actions are generated?

First, random attributes were assigned to a user when she/he was created, including:

- `adventurousness`: how likely is the user willing to try new things
- `wealthiness`: how much is the user willing to pay for a game
- `activeness`: how 'active' the user is
- `genre_affinity`: how much the user like a specific genre of games (e.g. RPG, FPS, Sports, etc.)

Then, the Generator will fake actions based on the said attributes. For example, a RPG lover (a user with high RPG affinity) is more likely to buy/play Skyrim V, while a FPS fan more likely to pick CS:GO; a rich player thinks it is acceptable to pay $19.99 for a game, while a user with low `wealthiness` may be reluctant to do so.
