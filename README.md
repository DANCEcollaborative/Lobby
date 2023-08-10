# Lobby

A lobby for dynamically joining individual students into shared socket rooms. 

# Parameters
Configuration parameters are all currently located near the top of file **lobby.py**. Parameter names with sample values are shown below.

- **targetUsersPerRoom** = 4 # Target/optimal # of users for room assignment
- **minUsersPerRoom** = 2 # Min users for suboptimal room assignment
- **maxUsersPerRoom** = 6 # Max users per rom 
- **maxWaitTimeForSubOptimalAssignment** = 10 # After user waits N seconds, attempt suboptimal assignment
- **maxWaitTimeUntilGiveUp** = 60 # Max seconds before giving up on assigning user to a room
- **maxRoomAgeForNewUsers** = 300 # Max room age (sec) after which no longer acccept new users
- **fillRoomsUnderTarget** = True # Fill any rooms with < target users before creating new room 
- **overFillRooms** = False # For suboptimal assignment, whether to overfill rooms (limited to maxUsersPerRoom)
- **urlPrefix** = "http://bazaar.lti.cs.cmu.edu/room" # A prefix for the room assignment URL
- **nextRoomNum** = 0 # The number after this will be the first room number assigned.

# To run

- Download the repo.
- Navigate to the top level of the downloaded repo.
- Enter the following commands:
  - docker build -t lobby . 
  - docker run -p 5000:5000 lobby
- In a browser, open multiple tabs/windows and navigate to
  - http:<MACHINE_ADDRESS>:5000/login/<USER_NAME>
  - where
    - <MACHINE_ADDRESS> is an address for the machine that Lobby is running on -- either a domain name (e.g., localhost) or an IP address (e.g., 127.0.0.1).
    - <USER_NAME> is a unique name for each tab or window.