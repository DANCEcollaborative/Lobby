import os
import time
from operator import attrgetter, itemgetter
import threading
import queue
from flask import Flask, request, url_for, jsonify, redirect, render_template
from flask_socketio import SocketIO, send, emit
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func

app = Flask(__name__)
socketio = SocketIO(app)


targetUsersPerRoom = 4
minUsersPerRoom = 2
maxUsersPerRoom = 6
maxWaitTimeForSubOptimalAssignment = 10          # seconds
maxWaitTimeUntilGiveUp = 60                    # seconds
maxRoomAgeForNewUsers = 300                     # seconds
fillRoomsUnderTarget = True
overFillRooms = False
urlPrefix = "http://bazaar.lti.cs.cmu.edu/room"
nextRoomNum = 0


rooms = []
availableRooms = []
roomsUnderTarget = []
# users = {}
unassignedUsers = []



# Queue to pass incoming users
user_queue = queue.Queue()

basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] =\
        'sqlite:///' + os.path.join(basedir, 'users.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

users = SQLAlchemy(app)

class User(users.Model):
    id = users.Column(users.Integer, primary_key=True)
    user_id = users.Column(users.String(30), nullable=False)
    name = users.Column(users.String(50))
    email = users.Column(users.String(80), unique=True, nullable=False)
    password = users.Column(users.String(30), primary_key=False)
    entity_id = users.Column(users.String(40), primary_key=False)
    agent = users.Column(users.String(30), primary_key=False)
    socket_id = users.Column(users.String(50))
    room = users.Column(users.String(50))
    # start_time = users.Column(time.time())
    start_time = users.Column(users.DateTime(timezone=True),
                           server_default=func.now())
    def __repr__(self):
        return f'<User {self.user_id}>'



# This /login step is TEMPORARY to use a single HTML page to SIMULATE receive logins from multiple users.
#     1. TEMPORARY step: Receive user_id from the request URL and send it back to the HTML page so the page can send a
#        unique user_id with each request. In browsers, each http://<SERVER>/login/<user_id> URL will have a unique
#        user_id value.
#     2. PERMANENT step: Receive user data in a user_connect request. For simulation testing, all the user data except
#        the user_id is a constant. For real world use, the "Launch OPE" button will send each data element customized
#        for the user.
#           -- Data:
#                   a. user_id (unique, as received from temporary step 1)
#                   b. name
#                   c. email
#                   d. password
#                   e. entityId
#                   f. Bazaar agent (I added this data requirement)
@app.route('/login/<user_id>', methods=['GET', 'POST'])
def login_get(user_id):
    print("Login: received user_id " + str(user_id))
    return render_template('lobby.html', user_id=user_id)


@socketio.on('user_connect')
def process_user_connect(user_data):
    socket_id = request.sid
    user_id = user_data.get('userId')
    name = user_data.get('name')
    email = user_data.get('email')
    password = user_data.get('password')
    entity_id = user_data.get('entityId')
    agent = user_data.get('agent')
    print(f"Client connected with socket_id: {socket_id} -- user_id: " + user_id)
    user_info = {'user_id': str(user_id), 'socket_id': str(socket_id), 'name': str(name), 'email': str(email), 'password': str(password), 'entity_id': str(entity_id), 'agent': str(agent)}
    user_queue.put(user_info)
    print("Login - user_queue length: " + str(user_queue.qsize()))
    emit('response_event', "Data received successfully")

@socketio.on('disconnect')
def process_disconnect():
    socket_id = request.sid
    print(f"Client DISCONNECTED -- socket_id: {socket_id}")


# @socketio.on('start_task')
# def start_task(user_id):
#     global users
#     print("start_task: received user_id " + str(user_id))
#     users[user_id]['socket_id'] = request.sid
#     user_room = users[user_id]['room']
#     print("start_task: user_room " + str(user_room))
#     if user_room is not None:
#         room_num = "room" + user_room['room_num']
#         print("start_task - emitting 'task_completed', message: " + str(room_num))
#         emit('task_completed', {'message': room_num}, room=request.sid)


# Shut down the consumer thread when the server stops
def shutdown_server():
    user_queue.put(None)
    consumer_thread.join()

def reassign_room(user, room):
    user_message = str(user['user_id']) + ": Return to URL " + room['url']
    print("reassign_room message: " + user_message)
    socketio.emit('update_event', {'message': user_message}, room=user['socket_id'])
#
# def print_uus():
#     print(unassignedUsers)

def assign_rooms():
    global unassignedUsers
    if len(unassignedUsers) > 0:
        if fillRoomsUnderTarget:
            fill_rooms_under_target()
        if len(unassignedUsers) >= targetUsersPerRoom:
            assign_new_rooms(targetUsersPerRoom)
        if (len(unassignedUsers) > 0) and overFillRooms:
            overfill_rooms()
        if len(unassignedUsers) > 0:
            users_due_for_suboptimal = get_users_due_for_suboptimal()
            # Expand to the following: If any users_due_for_suboptimal and enough unassigned
            if len(users_due_for_suboptimal) >= minUsersPerRoom:
                assign_new_room(len(users_due_for_suboptimal))
    unassignedUsers = prune_users()       # tell users who have been waiting too long to come back later



# If filling rooms that are under target before adding new rooms
def fill_rooms_under_target():
    # Assuming
    #   -- fill rooms that are most-under-target first
    #   -- fill one under-target room to target level before filling next under-target room
    global availableRooms, unassignedUsers
    availableRooms = prune_rooms(availableRooms)
    rooms_under_target = get_rooms_under_target()
    i = 0
    while (i < len(rooms_under_target)) and (len(unassignedUsers) > 0):
        assign_up_to_target(rooms_under_target[i])
        i += 1

def get_rooms_under_target():
    global availableRooms
    if len(availableRooms) == 0:
        return availableRooms
    availableRooms = prune_and_sort_rooms(availableRooms)
    rooms_under_target = []
    i = 0
    while i < len(availableRooms):
        if availableRooms[i]['num_users'] < targetUsersPerRoom:
            rooms_under_target.append(availableRooms[i])
        i += 1
    if len(rooms_under_target) > 0:
        return sort_rooms(rooms_under_target)
    else:
        return rooms_under_target

def get_users_due_for_suboptimal():
    global unassignedUsers
    users_due_for_suboptimal = []
    i = 0
    if len(unassignedUsers) > targetUsersPerRoom:   # Don't assign suboptimally if there are now target num of users
        return users_due_for_suboptimal
    while i < len(unassignedUsers):
        user_id = unassignedUsers[i]
        with app.app_context():
            user = User.query.filter_by(user_id=user_id).first()
        current_time = time.time()
        start_time = user.start_time
        time_diff = current_time - start_time
        print("Time diff for unassignedUser(" + user_id + ")" + str(time_diff))
        if time_diff > maxWaitTimeForSubOptimalAssignment:
            users_due_for_suboptimal.append(unassignedUsers[i])
        i += 1
    return users_due_for_suboptimal

def assign_up_to_target(room):
    global unassignedUsers
    while (room['num_users'] < targetUsersPerRoom) and (len(unassignedUsers) > 0):
        assign_room(unassignedUsers[0], room)

def add_to_room_under_target (room):
    global unassignedUsers
    room_users = room['users']
    num_under_target = targetUsersPerRoom - len(room_users)
    while (len(unassignedUsers) > 0) and (num_under_target > 0):
        user_id = unassignedUsers[0]
        assign_room(user_id,room)
        print("user_id " + str(user_id) + ": assigned to room_under_target " + room['url'])
        num_under_target -= 1

def assign_new_rooms(num_users_per_room):
    global unassignedUsers
    while len(unassignedUsers) > num_users_per_room:
        assign_new_room(num_users_per_room)

def assign_new_room(num_users):
    # url = ...        # CREATE SCHEME FOR ASSIGNING ROOM URLs
    global unassignedUsers, rooms, availableRooms, nextRoomNum
    nextRoomNum += 1
    url = urlPrefix + str(nextRoomNum)
    room = {'room_num': str(nextRoomNum), 'url': url, 'start_time': time.time(), 'users': [], 'num_users': 0}
    num_users_remaining = num_users
    while (num_users_remaining > 0) and (len(unassignedUsers) > 0):
        next_user_id = unassignedUsers[0]
        assign_room(next_user_id,room)
        num_users_remaining -= 1
    rooms.append(room)
    availableRooms.append(room)  # if no overfilling, room will soon be pruned from availableRooms

def overfill_rooms():
    global availableRooms, unassignedUsers
    availableRooms = prune_and_sort_rooms(availableRooms)
    if len(availableRooms) == 0:
        return
    next_user_id = unassignedUsers[0]  # users are listed in increasing start_time order
    with app.app_context():
        next_user = User.query.filter_by(user_id=next_user_id).first()
    longest_user_wait = time.time() - next_user.start_time
    while (longest_user_wait > maxWaitTimeForSubOptimalAssignment) and (len(availableRooms) > 0):
        next_room = availableRooms[0]
        assign_room(next_user_id, next_room)
        availableRooms = prune_and_sort_rooms(availableRooms)
        if len(unassignedUsers) > 0:
            next_user_id = unassignedUsers[0]
            with app.app_context():
                next_user = User.query.filter_by(user_id=next_user_id).first()
            longest_user_wait = time.time() - next_user.start_time
        else:
            longest_user_wait = 0   # No more users waiting

def assign_room(user_id,room):
    # Send user link to user_room
    global unassignedUsers
    with app.app_context():
        user = User.query.filter_by(user_id=user_id).first()
        user.room = room
        users.session.add(user)
        users.session.commit()
    room['users'].append(user_id)
    room['num_users'] = len(room['users'])
    unassignedUsers.remove(user_id)
    user_message = str(user_id) + ": Go to URL " + room['url']
    print("assign_room: socket_id: " + user.socket_id + "    message: " + user_message)
    socketio.emit('update_event', {'message': user_message}, room=user.socket_id)


def prune_and_sort_rooms(room_list):
    pruned_rooms = prune_rooms(room_list)
    if len(pruned_rooms) != 0:
        return sort_rooms(pruned_rooms)

def prune_rooms(room_list):
    num_rooms = len(room_list)
    i = 0
    while i < num_rooms:
        room = room_list[i]
        room_age = time.time() - room['start_time']
        room_users = room['users']
        room_slots_available = maxUsersPerRoom - len(room_users)
        # Prune rooms that are too old for new users or that are filled to max
        if (room_age >= maxRoomAgeForNewUsers) or (room_slots_available <= 0):
            del room_list[i]
            num_rooms = len(room_list)
        else:           # increment room counter 'i' only if current room was not deleted
            i += 1
    return room_list

# Sort primarily by number of users (ascending), then secondarily by start_time (ascending)
#   to prioritize rooms with the least users, and secondarily with the oldest start times
def sort_rooms(room_list):
    s = sorted(room_list, key=itemgetter('start_time'))
    return sorted(s,key=itemgetter('num_users'))

def prune_users():
    global unassignedUsers
    for user_id in unassignedUsers:
        with app.app_context():
            user = User.query.filter_by(user_id=user_id).first()
        if (time.time() - user.start_time) >= maxWaitTimeUntilGiveUp:
            user_message = str(user.user_id) + ": I'm sorry. There are not enough other users logged in right now to start a session. Please try again later."
            print("prune_users: socket_id: " + user.socket_id + "    message: " + user_message)
            socketio.emit('update_event', {'message': user_message}, room=user.socket_id)
            unassignedUsers.remove(user_id)
    return unassignedUsers

def print_room_assignments():
    global rooms
    if len(rooms) > 0:
        for room in rooms:
            print("Room url: " + room['url'])
            for user_id in room['users']:
                print("   User: " + user_id)
    else:
        print("No rooms yet")



# Worker function for the consumer
def assigner():
    global users, unassignedUsers
    while True:
        print("Entering assigner")
        while not user_queue.empty():
            print("assigner: queue not empty")
            user_info = user_queue.get()
            user_id = user_info['user_id']
            socket_id = user_info['socket_id']
            name = user_info['name']
            email = user_info['email']
            password = user_info['password']
            entity_id = user_info['entity_id']
            agent = user_info['agent']
            if user_id is None:
                print("assigner: user_id is None")
                break
            if socket_id is None:
                print("assigner: socket_id is None")
                break
            else:
                with app.app_context():
                    users.create_all()

                    # If user has previously logged in
                    user = User.query.filter_by(user_id=user_id).first()
                    if user is not None:
                        print("assigner: user " + str(user_id) + " has previously logged in")
                        room = user.room
                        user.socket_id = socket_id
                        user.session.add(user)
                        user.session.commit()

                        # If user already assigned to a room, reassign
                        if room is not None:
                            print("assigner: user " + str(user_id) + " already has room " + str(room))
                            reassign_room(user_id, room)

                        # else previously logged-in user should already be in the unassignedUsers list
                        #      -- but double-checking as a failsafe
                        elif user_id not in unassignedUsers:
                            unassignedUsers.append(user_id)

                    # This is a new user
                    else:
                        # user = {'user_id': user_id, 'socket_id': socket_id, 'start_time': time.time(), 'room': None,
                        #         'name': name, 'email': email, 'password': password, 'entity_id': entity_id,'agent': agent}

                        user = User(user_id=user_id, name=name, email=email, password=password,
                                    entity_id=entity_id, agent=agent, socket_id=socket_id)
                        print("assigner - user " + user.user_id + " start_time: " + str(user.start_time) + " room: None  socket_id: " + user.socket_id + "  name: " + user.name)
                        print("assigner - time.time(): " + str(time.time()))
                        users.create_all()
                        users.session.add(user)
                        users.session.commit()
                        unassignedUsers.append(user_id)
                        print("assigner - len(unassignedUsers) = " + str(len(unassignedUsers)))

        if len(unassignedUsers) > 0:
            assign_rooms()
            print_room_assignments()
        time.sleep(1)


# Create and start the consumer thread
consumer_thread = threading.Thread(target=assigner)
consumer_thread.start()



if __name__ == '__main__':
    # with app.app_context():
    #     users.create_all()
    socketio.run(app, threaded=True, port=5000)
    # When the server is shut down, stop the consumer thread as well
    shutdown_server()
