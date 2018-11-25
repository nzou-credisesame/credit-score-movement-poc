## Credit score movement, action recommendation engine

### Motivation
The goal of score movement project is to analyze and to understand the actions that users took within different profile segments. 


Based on the analysis results, Credit Sesame can suggest the most effective actions for users to follow and pitfalls/actions to avoid, eventually bring more users to a quick credit improvement experience.

### Project Status: poc

### Project
>Project 1: Medium Increase Group (30-50p) and High Increase Group (50-100p) Comparison [ASANA ticket](https://app.asana.com/0/883289177114008/883289177114015)
      
>Project 2: (Follow up of Project 1) Initial CS 575-625, >0 open credit card comparison [ASANA ticket](https://app.asana.com/0/883289177114008/899844403710320)

>Project 3: (POC of action recommendation system) Initial CS 550-650, Score Movement groups Comparison



### Code structure:
- Util.py to connect Redshift and local machine
- SQL to extract user profiles from redshift (0-x.sql)
- Python to analyze statistically significant feature (feature selection) via Decision Tree, Random Forest (1-featureselection.py)
- Python to visualize 'One Variable at a Time' (2-analytics.py)


  
### Results
[Excel](https://docs.google.com/spreadsheets/d/1THX2HWMX-7I7LsGl6WRc7QVK6PUSTYxgmN4zmR8qbKg/edit#gid=0)

[Slides](https://docs.google.com/presentation/d/1dmb6i4BL3jDMRQFUOcLBhSp67c8JgdMabdtWhPiZVQo/edit#slide=id.g47e7b63294_0_0)
