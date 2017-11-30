""" Script to start data collection process """

"""
STEPS:
1. Create Queues
2. Start Consumers in their threads
3. Start populating first Queue with ID's from BOM
4. Once all pages have been checked and all Queues are empty...
5. Send kill commands to consumers
6. Add Films to Film.all_films
7. Add Actors to Actor.all_actors
8. For each film, get aggregates
9. Save to MongoDB
"""