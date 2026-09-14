# Lobby

A lobby for dynamically joining individual students into shared socket rooms. 

# Parameters
Configuration parameters are all currently located near the top of file **lobby.py**, where tbey may be hardcoded . Many parameters can also be updated dynamically using curl commands formatted as follows: `curl -X PUT <lobby server URL>/<dynamic parameter name (see below)>/<parameter value>`. Some commonly used dynamic parameter names with sample values are shown below. 

- **help** = (No parameter value.) Simply provides a key for updating Lobby parameters. 
- **targetUsers** = 4 - Target/optimal # of users for room assignment
- **minUsers** = 2 - Min users for suboptimal room assignment
- **maxUsers** = 5 - Max users per room 
- **subassignWait** = 10 - After user waits N seconds, attempt suboptimal assignment
- **roomNum** = 0 - The number after this will be the first room number assigned.
- **giveUpWait** = 300 - Max seconds before giving up on assigning user to a room
- **maxRoomAge** = 600 - Max room age (sec) after which no longer acccept new users
- **requestPrefix** = The URL of the server from which the Lobby is requesting a session. 
- **moduleSlug** - An identifier for the specific content requested from the server.

# To run

- Download the repo.
- Navigate to the top level of the downloaded repo.
- Enter the following command:
  - docker compose up --build -d