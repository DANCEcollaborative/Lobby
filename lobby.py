import os
import time
from datetime import datetime
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
urlPrefix = "http://bazaar.lti.cs.cmu.edu/"
roomPrefix = "room"
nextRoomNum = 0
lobby_initialized = False
waiting_room = None
lobby_attendant = None


rooms = []
availableRooms = []
roomsUnderTarget = []
# lobby_db = {}
unassignedUsers = []



# Queue to pass incoming users
user_queue = queue.Queue()

basedir = os.path.abspath(os.path.dirname(__file__))
# app.config['SQLALCHEMY_DATABASE_URI'] =\
#         'sqlite:///' + os.path.join(basedir, 'lobby_db.db')
app.config['SQLALCHEMY_DATABASE_URI'] =\
        'postgresql://postgres:testpwd@lobby_db:5432/lobby_db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

lobby_db = SQLAlchemy(app)

class Room(lobby_db.Model):
    id = lobby_db.Column(lobby_db.Integer, primary_key=True)
    room_name = lobby_db.Column(lobby_db.String(50))
    url = lobby_db.Column(lobby_db.String(200))
    start_time = lobby_db.Column(lobby_db.DateTime(timezone=False),
                            server_default=func.now())
    users = lobby_db.relationship('User', back_populates='room')
    # start_time = lobby_db.Column(lobby_db.DateTime(timezone=True),
    #                         server_default=func.now())
    # start_time = lobby_db.Column(lobby_db.Integer, default=int(datetime.now().timestamp()),
    #                        server_default=func.now())
    # users = lobby_db.relationship('User', backref='room', lazy=True)
    def __repr__(self):
        return f'<Room {self.room_name}>'

class User(lobby_db.Model):
    id = lobby_db.Column(lobby_db.Integer, primary_key=True)
    user_id = lobby_db.Column(lobby_db.String(30), nullable=False)
    name = lobby_db.Column(lobby_db.String(50), primary_key=False)
    email = lobby_db.Column(lobby_db.String(80), primary_key=False)
    password = lobby_db.Column(lobby_db.String(30), primary_key=False)
    entity_id = lobby_db.Column(lobby_db.String(40), primary_key=False)
    agent = lobby_db.Column(lobby_db.String(30), primary_key=False)
    socket_id = lobby_db.Column(lobby_db.String(50))
    start_time = lobby_db.Column(lobby_db.DateTime(timezone=False),
                            server_default=func.now())
    room_name = lobby_db.Column(lobby_db.String(50))
    room_id = lobby_db.Column(lobby_db.Integer, lobby_db.ForeignKey('room.id'), nullable=True)
    room = lobby_db.relationship('Room', back_populates='users')
    # room_name = lobby_db.Column(lobby_db.String(50), primary_key=False)
    # room_name = lobby_db.Column(lobby_db.String(50))
    # start_time = lobby_db.Column(lobby_db.DateTime(timezone=True),
    #                        server_default=func.now())
    # start_time = lobby_db.Column(lobby_db.Integer, default=int(datetime.now().timestamp()),
    #                        server_default=func.now())
    # room_name = lobby_db.Column(lobby_db.Integer, db.ForeignKey('room.id'), nullable=False)
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


# Shut down the consumer thread when the server stops
def shutdown_server():
    user_queue.put(None)
    consumer_thread.join()

def reassign_room(user_id, room):
    with app.app_context():
        user = User.query.filter_by(user_id=user_id).first()
        user_message = str(user.user_id) + ": Return to URL " + room['url']
        print("reassign_room message: " + user_message)
        print("reassign_room socket_id: " + str(user.socket_id))
        socketio.emit('update_event', {'message': user_message}, room=user.socket_id)

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
        time_diff = time.time() - user.start_time.timestamp()
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
    room_name = roomPrefix + str(nextRoomNum)
    url = urlPrefix + room_name
    room = Room(room_name=room_name, url=url)
    # room = {'room_name': room_name, 'url': url, 'start_time': time.time(), 'users': [], 'num_users': 0}
    # unassigned_users = session.query(User).filter_by(room=None).order_by(User.start_time.asc()).all()
    num_users_remaining = num_users
    while (num_users_remaining > 0) and (len(unassignedUsers) > 0):
        next_user = unassignedUsers[0]
        assign_room(next_user,room)
        num_users_remaining -= 1
    # rooms.append(room)
    # availableRooms.append(room)  # if no overfilling, room will soon be pruned from availableRooms

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

def assign_room(user,room):
    # Send user link to user_room
    # global unassignedUsers
    # global lobby_db
    room.users.append(user)
    lobby_db.session.add(room)
    lobby_db.session.commit()

    # with app.app_context():
    #     user = User.query.filter_by(user_id=user_id).first()
    #     user.room_id = room['room_name']
    #     socket_id = user.socket_id
    #     lobby_db.session.add(user)
    #     lobby_db.session.commit()
    # room['users'].append(user_id)
    # room['num_users'] = len(room['users'])
    # unassignedUsers.remove(user_id)
    user_message = str(user.user_id) + ": Go to URL " + str(room.url)
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
        if (time.time() - user.start_time.timestamp()) >= maxWaitTimeUntilGiveUp:
            user_message = str(user.user_id) + ": I'm sorry. There are not enough other users logged in right now to start a session. Please try again later."
            print("prune_users: socket_id: " + user.socket_id + "    message: " + user_message)
            socketio.emit('update_event', {'message': user_message}, room=user.socket_id)
            unassignedUsers.remove(user_id)
    return unassignedUsers

def get_room_by_id(room_name):
    room = None
    num_rooms = len(rooms)
    for i in range(num_rooms):
        if rooms[i]['room_name'] == room_name:
            return rooms[i]
    return room

def print_room_assignments():
    # global rooms
    created_rooms = Room.query.all()
    if len(created_rooms) > 0:
        for room in created_rooms:
            print("Room " + str(room.room_name))
            for user in room.users:
                print("   " + user.user_id)
    else:
        print("No rooms yet")

def print_users():
    # global rooms
    users = User.query.all()
    if len(users) > 0:
        for user in users:
            print("User: " + str(user.user_id))
    else:
        print("No rooms yet")

def update_user_socket(user_id, socket_id):
    with app.app_context():
        lobby_db.create_all()
        user = User.query.filter_by(user_id=user_id).first()
        user.socket_id = socket_id
        user.session.add(user)
        user.session.commit()

# Worker function for the consumer
def assigner():
    # global lobby_db, unassignedUsers
    global lobby_db, lobby_initialized, waiting_room, lobby_attendant
    user_created = False
    if not lobby_initialized:
        with app.app_context():
            print("assigner inside 'with'")
            lobby_db.drop_all()
            lobby_db.create_all()
            waiting_room = Room(room_name="waiting_room", url="null")
            print("assigner #1, waiting_room - room_name: " + waiting_room.room_name)
            lobby_attendant = User(user_id="lobby_attendant", name="Lobby Attendant", email="null", password="null",
                                   entity_id="null", agent="null", socket_id="null", room_name="waiting_room",
                                   room=waiting_room)
            lobby_db.session.add(waiting_room)
            lobby_db.session.add(lobby_attendant)
            lobby_db.session.commit()
            waiting_room_copy = Room.query.filter_by(room_name="waiting_room").first()
            print("assigner #2, waiting_room_copy - room_name: " + waiting_room_copy.room_name)
        lobby_initialized = True
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
                    # lobby_db.create_all()
                    user = User.query.filter_by(user_id=user_id).first()

                    # If user has previously logged in
                    # if False:   # TEMP DISABLING LOGIC BELOW
                    if user is not None:
                        print("assigner: user " + str(user_id) + " has previously logged in. NEW socket_id: " + str(socket_id))
                        room = Room.query().get(user.room_id)
                        print("                   start_time: " + str(user.start_time) + "  room: " + str(user.room_id) + "  room_name: " + room.room_name + "  OLD socket_id: " + user.socket_id + "  name: " + user.name)
                        # lobby_db.create_all()
                        room_id = user.room_id
                        # update_user_socket(user_id, socket_id)
                        user.socket_id = socket_id
                        lobby_db.session.add(user)
                        lobby_db.session.commit()
                        print("assigner: user " + str(user_id) + " UPDATED socket_id: " + str(user.socket_id))

                        # If user already assigned to a room, reassign
                        if room_id is not None:
                            print("assigner: user " + str(user_id) + " already has room " + room.room_name)
                            reassign_room(user_id, room.room_name)

                        # else previously logged-in user should already be in the unassignedUsers list
                        #      -- but double-checking as a failsafe
                        elif user_id not in unassignedUsers:
                            unassignedUsers.append(user_id)

                    # This is a new user
                    else:
                        # user = {'user_id': user_id, 'socket_id': socket_id, 'start_time': time.time(), 'room': None,
                        #         'name': name, 'email': email, 'password': password, 'entity_id': entity_id,'agent': agent}

                        # waiting_room_local = Room.query.filter_by(room_name="waiting_room").first()
                        # print("assigner, new user: waiting_room:  room_name: " + waiting_room_local.room_name)
                        user = User(user_id=user_id, name=name, email=email, password=password,
                                    entity_id=entity_id, agent=agent, socket_id=socket_id, room=waiting_room,
                                    room_name=waiting_room.room_name)
                        # user = User(user_id=user_id, name=name, email=email, password=password,
                        #             entity_id=entity_id, agent=agent, socket_id=socket_id)
                        user_created = True
                        print("assigner - user " + user.user_id + " start_time: " + str(user.start_time) + "   room_name: " + waiting_room.room_name + "   socket_id: " + user.socket_id + "   name: " + user.name)
                        print("assigner - time.time(): " + str(time.time()))
                        lobby_db.session.add(user)
                        lobby_db.session.commit()
                        # unassignedUsers.append(user_id)
                        # print("assigner - len(unassignedUsers) = " + str(len(unassignedUsers)))

            if user_created:
                with app.app_context():
                    unassigned_users = User.query.filter_by(room_name="waiting_room").order_by(User.start_time.asc()).all()
                    if len(unassignedUsers) > 0:
                        assign_rooms()
                        print_users()
                        print_room_assignments()
                    else:
                        print("assigner: user_created but len(unassignedUsers) == 0")
        time.sleep(1)


# Create and start the consumer thread
consumer_thread = threading.Thread(target=assigner)
consumer_thread.start()


if __name__ == '__main__':
    # print("main entry")
    # with app.app_context():
    #     print("main inside 'with'")
    #     lobby_db.create_all()
    #     waiting_room = Room(room_name="waiting_room", url="null")
    #     print("main #1, waiting_room - room_name: " + waiting_room.room_name)
    #     lobby_attendant = User(user_id="lobby_attendant", name="Lobby Attendant", email="null", password="null",
    #         entity_id="null", agent="null", socket_id="null", room_name="waiting_room", room=waiting_room)
    #     lobby_db.session.add(waiting_room)
    #     lobby_db.session.add(lobby_attendant)
    #     lobby_db.session.commit()
    #     waiting_room_copy = Room.query.filter_by(room_name="waiting_room").first()
    #     print("main #1, waiting_room_copy - room_name: " + waiting_room_copy.room_name)
    # print("main after 'with'")
    socketio.run(app, threaded=True, port=5000)
    # When the server is shut down, stop the consumer thread as well
    shutdown_server()
