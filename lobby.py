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
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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
unassigned_users = []

session = None

# Queue to pass incoming users
user_queue = queue.Queue()

basedir = os.path.abspath(os.path.dirname(__file__))
# app.config['SQLALCHEMY_DATABASE_URI'] =\
#         'sqlite:///' + os.path.join(basedir, 'lobby_db.db')
app.config['SQLALCHEMY_DATABASE_URI'] =\
        'postgresql://postgres:testpwd@lobby_db:5432/lobby_db'
# engine = create_engine('postgresql://postgres:testpwd@lobby_db:5432/lobby_db', echo=True)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False



# Session = sessionmaker(bind=engine)
# session = Session()

lobby_db = SQLAlchemy(app)

class Room(lobby_db.Model):
    __tablename__ = 'room'
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
    __tablename__ = 'user'
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
    print("Login: received user_id " + str(user_id), flush=True)
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
    print(f"Client connected with socket_id: {socket_id} -- user_id: " + user_id, flush=True)
    user_info = {'user_id': str(user_id), 'socket_id': str(socket_id), 'name': str(name), 'email': str(email), 'password': str(password), 'entity_id': str(entity_id), 'agent': str(agent)}
    user_queue.put(user_info)
    print("Login - user_queue length: " + str(user_queue.qsize()), flush=True)
    emit('response_event', "Data received successfully")

@socketio.on('disconnect')
def process_disconnect():
    socket_id = request.sid
    print(f"Client DISCONNECTED -- socket_id: {socket_id}", flush=True)


# Shut down the consumer thread when the server stops
def shutdown_server():
    user_queue.put(None)
    consumer_thread.join()

def reassign_room(user_id, room):
    with app.app_context():
        user = User.query.filter_by(user_id=user_id).first()
        user_message = str(user.user_id) + ": Return to URL " + room['url']
        print("reassign_room message: " + user_message, flush=True)
        print("reassign_room socket_id: " + str(user.socket_id), flush=True)
        socketio.emit('update_event', {'message': user_message}, room=user.socket_id)

def assign_rooms():
    global unassigned_users
    if len(unassigned_users) > 0:
        if fillRoomsUnderTarget:
            fill_rooms_under_target()
        if len(unassigned_users) >= targetUsersPerRoom:
            assign_new_rooms(targetUsersPerRoom)
        if (len(unassigned_users) > 0) and overFillRooms:
            overfill_rooms()
        if len(unassigned_users) > 0:
            users_due_for_suboptimal = get_users_due_for_suboptimal()
            # Expand to the following: If any users_due_for_suboptimal and enough unassigned
            if len(users_due_for_suboptimal) >= minUsersPerRoom:
                assign_new_room(len(users_due_for_suboptimal))
    unassigned_users = prune_users()       # tell users who have been waiting too long to come back later



# If filling rooms that are under target before adding new rooms
def fill_rooms_under_target():
    # Assuming
    #   -- fill rooms that are most-under-target first
    #   -- fill one under-target room to target level before filling next under-target room
    global availableRooms, unassigned_users
    availableRooms = prune_rooms(availableRooms)
    rooms_under_target = get_rooms_under_target()
    i = 0
    while (i < len(rooms_under_target)) and (len(unassigned_users) > 0):
        assign_up_to_target(rooms_under_target[i])
        i += 1

# FIX THIS !!!
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
    global unassigned_users
    users_due_for_suboptimal = []
    i = 0
    if len(unassigned_users) > targetUsersPerRoom:   # Don't assign suboptimally if there are now target num of users
        print("get_users_due_for_suboptimal(): now there are enough users for the target", flush=True)
        return users_due_for_suboptimal
    with app.app_context():
        while i < len(unassigned_users):
            # user_id = unassigned_users[i]
            user = unassigned_users[i]
            # print("get_users_due_for_suboptimal(): checking user_id = " + str(user.user_id), flush=True)
            # user = User.query.get(user_id)
            time_diff = time.time() - user.start_time.timestamp()
            print("Time diff for unassignedUser(" + user.user_id + "): " + str(time_diff), flush=True)
            if time_diff > maxWaitTimeForSubOptimalAssignment:
                users_due_for_suboptimal.append(unassigned_users[i])
            i += 1
    return users_due_for_suboptimal

# FIX THIS !!!
def assign_up_to_target(room):
    global unassigned_users
    while (room['num_users'] < targetUsersPerRoom) and (len(unassigned_users) > 0):
        assign_room(unassigned_users[0], room)

def add_to_room_under_target (room):
    global unassigned_users
    with app.app_context():
        room_users = room['users']
        num_under_target = targetUsersPerRoom - len(room_users)
        while (len(unassigned_users) > 0) and (num_under_target > 0):
            user_id = unassigned_users[0]
            assign_room(user_id,room)
            print("user_id " + str(user_id) + ": assigned to room_under_target " + room['url'], flush=True)
            num_under_target -= 1

def assign_new_rooms(num_users_per_room):
    global unassigned_users
    while len(unassigned_users) > num_users_per_room:
        assign_new_room(num_users_per_room)

def assign_new_room(num_users):
    # url = ...        # CREATE SCHEME FOR ASSIGNING ROOM URLs
    global unassigned_users, rooms, availableRooms, nextRoomNum
    nextRoomNum += 1
    room_name = roomPrefix + str(nextRoomNum)
    url = urlPrefix + room_name
    with app.app_context():
        room = Room(room_name=room_name, url=url)
        lobby_db.session.add(room)
        lobby_db.session.commit()
        # room = {'room_name': room_name, 'url': url, 'start_time': time.time(), 'users': [], 'num_users': 0}
        # unassigned_users = session.query(User).filter_by(room=None).order_by(User.start_time.asc()).all()
        num_users_remaining = num_users
        while (num_users_remaining > 0) and (len(unassigned_users) > 0):
            print("assign_new_room, len(unassigned_users): " + str(len(unassigned_users)))
            print("assign_new_room, num_users_remaining: " + str(num_users_remaining))
            next_user = unassigned_users[0]
            # assign_room(next_user,room.id)
            assign_room(next_user,room)
            unassigned_users.remove(next_user)
            num_users_remaining -= 1
        # rooms.append(room)
        # availableRooms.append(room)  # if no overfilling, room will soon be pruned from availableRooms

def overfill_rooms():
    global availableRooms, unassigned_users
    availableRooms = prune_and_sort_rooms(availableRooms)
    if len(availableRooms) == 0:
        return
    next_user_id = unassigned_users[0]  # users are listed in increasing start_time order
    with app.app_context():
        next_user = User.query.filter_by(user_id=next_user_id).first()
        longest_user_wait = time.time() - next_user.start_time
        while (longest_user_wait > maxWaitTimeForSubOptimalAssignment) and (len(availableRooms) > 0):
            next_room = availableRooms[0]
            assign_room(next_user_id, next_room)
            availableRooms = prune_and_sort_rooms(availableRooms)
            if len(unassigned_users) > 0:
                next_user_id = unassigned_users[0]
                # with app.app_context():
                next_user = User.query.filter_by(user_id=next_user_id).first()
                longest_user_wait = time.time() - next_user.start_time
            else:
                longest_user_wait = 0   # No more users waiting

def assign_room(user,room):
    # Send user link to user_room
    global unassigned_users
    # global lobby_db
    # with app.app_context():
    # unassigned_users.remove(user)

    # with app.app_context():
        #     user = User.query.filter_by(user_id=user_id).first()
        #     user.room_id = room['room_name']
        #     socket_id = user.socket_id
        #     lobby_db.session.add(user)
        #     lobby_db.session.commit()
        # room['users'].append(user_id)
        # room['num_users'] = len(room['users'])
        # unassigned_users.remove(user_id)
    room.users.append(user)
    user.room_name = room.room_name
    lobby_db.session.add(room)
    lobby_db.session.commit()
    user_message = str(user.user_id) + ": Go to URL " + str(room.url)
    print("assign_room: socket_id: " + user.socket_id + "    message: " + user_message, flush=True)
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
    global unassigned_users
    # for user_id in unassigned_users:
    for user in unassigned_users:
        with app.app_context():
            #     user = User.query.filter_by(user_id=user_id).first()
            if (time.time() - user.start_time.timestamp()) >= maxWaitTimeUntilGiveUp:
                user_message = str(user.user_id) + ": I'm sorry. There are not enough other users logged in right now to start a session. Please try again later."
                print("prune_users: socket_id: " + user.socket_id + "    message: " + user_message, flush=True)
                socketio.emit('update_event', {'message': user_message}, room=user.socket_id)
                unassigned_users.remove(user)
    return unassigned_users

def get_room_by_id(room_name):
    room = None
    num_rooms = len(rooms)
    for i in range(num_rooms):
        if rooms[i]['room_name'] == room_name:
            return rooms[i]
    return room

def print_room_assignments():
    # global rooms
    with app.app_context():
        created_rooms = Room.query.all()
        if len(created_rooms) > 0:
            for room in created_rooms:
                print("Room " + str(room.room_name), flush=True)
                for user in room.users:
                    print("   " + user.user_id, flush=True)
        else:
            print("No rooms yet", flush=True)

def print_users():
    # global rooms
    with app.app_context():
        users = User.query.all()
        if len(users) > 0:
            for user in users:
                print("User: " + str(user.user_id), flush=True)
        else:
            print("No rooms yet", flush=True)

def update_user_socket(user_id, socket_id):
    global session
    with app.app_context():
        #  lobby_db.create_all()
        user = User.query.filter_by(user_id=user_id).first()
        user.socket_id = socket_id
        session.add(user)
        session.commit()

# Worker function for the consumer
def assigner():
    # global lobby_db, unassigned_users
    global lobby_db, lobby_initialized, waiting_room, lobby_attendant, unassigned_users, session

    user_created = False
    if not lobby_initialized:
        with app.app_context():
            session = lobby_db.session
            # print("assigner inside 'with'", flush=True)
            lobby_db.drop_all()
            lobby_db.create_all()
            waiting_room = Room(room_name="waiting_room", url="null")
            print("assigner - Created waiting_room - room_name: " + waiting_room.room_name, flush=True)
            # lobby_attendant = User(user_id="lobby_attendant", n
            # ame="Lobby Attendant", email="null", password="null",
            #                        entity_id="null", agent="null", socket_id="null", room_name="waiting_room",
            #                        room=waiting_room)
            session.add(waiting_room)
            # lobby_db.session.add(lobby_attendant)
            session.commit()
            lobby_initialized = True
    while True:
        print("Entering assigner", flush=True)
        while not user_queue.empty():
            print("assigner: queue not empty", flush=True)
            user_info = user_queue.get()
            user_id = user_info['user_id']
            socket_id = user_info['socket_id']
            name = user_info['name']
            email = user_info['email']
            password = user_info['password']
            entity_id = user_info['entity_id']
            agent = user_info['agent']
            if user_id is None:
                print("assigner: user_id is None", flush=True)
                break
            if socket_id is None:
                print("assigner: socket_id is None", flush=True)
                break
            else:
                with app.app_context():
                    # lobby_db.create_all()
                    user = User.query.filter_by(user_id=user_id).first()

                    # If user has previously logged in
                    # if False:   # TEMP DISABLING LOGIC BELOW
                    if user is not None:
                        print("assigner: user " + str(user_id) + " has previously logged in. NEW socket_id: " + str(socket_id), flush=True)
                        room_id = user.room_id
                        room = Room.query.get(room_id)
                        room_name = room.room_name
                        print("assigner; user is not none - room_name: " + room_name, flush=True)
                        # room = app.app_context.session.get(Room,user.room_id)
                        print("                   start_time: " + str(user.start_time) + "  room: " + str(user.room_id) + "  room_name: " + room.room_name + "  OLD socket_id: " + user.socket_id + "  name: " + user.name, flush=True)
                        # lobby_db.create_all()
                        room_id = user.room_id
                        # update_user_socket(user_id, socket_id)
                        user.socket_id = socket_id
                        session.add(user)
                        session.commit()
                        print("assigner: user " + str(user_id) + " UPDATED socket_id: " + str(user.socket_id), flush=True)

                        # If user already assigned to a room, reassign
                        if room_id is not None and room_name != "waiting_room":
                            print("assigner: user " + str(user_id) + " already has room " + room.room_name, flush=True)
                            reassign_room(user_id, room.room_name)

                        # else previously logged-in user should already be in the unassigned_users list
                        #      -- but double-checking as a failsafe
                        elif user_id not in unassigned_users:
                            unassigned_users.append(user_id)

                    # This is a new user
                    else:
                        # user = {'user_id': user_id, 'socket_id': socket_id, 'start_time': time.time(), 'room': None,
                        #         'name': name, 'email': email, 'password': password, 'entity_id': entity_id,'agent': agent}

                        # waiting_room_local = Room.query.filter_by(room_name="waiting_room").first()
                        # print("assigner, new user: waiting_room:  room_name: " + waiting_room_local.room_name)
                        # with app.app_context():
                        waiting_room = Room.query.filter_by(room_name="waiting_room").first()
                        user = User(user_id=user_id, name=name, email=email, password=password,
                                    entity_id=entity_id, agent=agent, socket_id=socket_id, room=waiting_room,
                                    room_name=waiting_room.room_name)
                        # user = User(user_id=user_id, name=name, email=email, password=password,
                        #             entity_id=entity_id, agent=agent, socket_id=socket_id)
                        print("assigner - user " + user.user_id + " start_time: " + str(user.start_time) + "   room_name: " + waiting_room.room_name + "   socket_id: " + user.socket_id + "   name: " + user.name, flush=True)
                        print("assigner - time.time(): " + str(time.time()), flush=True)
                        session.add(user)
                        session.commit()
                        # unassigned_users.append(user_id)
                        # print("assigner - len(unassigned_users) = " + str(len(unassigned_users)))

        with app.app_context():
            unassigned_users = User.query.filter_by(room_name="waiting_room").order_by(User.start_time.asc()).all()
        if len(unassigned_users) > 0:
            # with app.app_context():
            print("assigner -- unassigned_users: ", flush=True)
            for i in range(len(unassigned_users)):
                print("  user.id: " + str(unassigned_users[i]), flush=True)
            assign_rooms()
            print_users()
            print_room_assignments()

        time.sleep(1)


# Create and start the consumer thread
consumer_thread = threading.Thread(target=assigner)
consumer_thread.start()


if __name__ == '__main__':
    # global session, lobby_db
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

    session = lobby_db.session

    # SEEMS TO WORK
    # with app.app_context():
    #     engine = create_engine()
    #     Session = sessionmaker(bind=engine)
    #     session = Session()


    socketio.run(app, threaded=True, port=5000)
    # When the server is shut down, stop the consumer thread as well
    shutdown_server()
