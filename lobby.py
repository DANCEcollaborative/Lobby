import os
import time
from datetime import datetime
import threading
import queue
import json
import requests
# import re
from flask import Flask, request, render_template, jsonify
from flask_socketio import SocketIO, emit
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func
# from sqlalchemy.orm import sessionmaker

app = Flask(__name__)
socketio = SocketIO(app)

# Queue to pass incoming users
user_queue = queue.Queue()

targetUsersPerRoom = 4
minUsersPerRoom = 2
maxUsersPerRoom = 5
maxWaitTimeForSubOptimalAssignment = 10        # seconds
maxWaitTimeUntilGiveUp = 70                    # seconds    >>> UPDATE THIS <<<
maxRoomAgeForNewUsers = 60                     # seconds    >>> UPDATE THIS <<<
fillRoomsUnderTarget = True
overFillRooms = True
lobby_initialized = False
unassigned_users = []
assigner_sleep_time = 1     # sleep time in seconds between assigner iterations

# fake_session_url = "https://bazaar.lti.cs.cmu.edu/"
fake_session_url = '<a href="https://bazaar.lti.cs.cmu.edu">Go to Room</a>'
lobby_url_prefix = 'https://bazaar.lti.cs.cmu.edu/sail-lobby/'
generalRequestPrefix = 'https://ope.sailplatform.org/api/v1'
sessionRequestPath = 'opesessions'
userRequestPath = 'opeusers'
sessionReadinessPath = 'sessionReadiness'
roomPrefix = "room"
nextRoomNum = 0
moduleSlug = 'default'
opeBotNamespace = 'default'
opeBotName = 'bazaar-lti-at-cs-cmu-edu'

# opeBotUsername = 'user-at-andrew-cmu-edu'  # UPDATE THIS ???
opeBotUsername = 'bazaar-lti-cs-cmu-edu'  # UPDATE THIS ???

basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] =\
        'postgresql://postgres:testpwd@lobby_db:5432/lobby_db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_EXPIRE_ON_COMMIT'] = False
lobby_db = SQLAlchemy(app)
session = None
sessionUpdated = False


class Room(lobby_db.Model):
    __tablename__ = 'room'
    id = lobby_db.Column(lobby_db.Integer, primary_key=True)
    room_name = lobby_db.Column(lobby_db.String(50))
    session_url = lobby_db.Column(lobby_db.String(200))
    module_slug = moduleSlug
    bot_namespace = opeBotNamespace
    bot_name = opeBotName
    start_time = lobby_db.Column(lobby_db.DateTime(timezone=False), server_default=func.now())
    num_users = lobby_db.Column(lobby_db.Integer)
    users = lobby_db.relationship('User', back_populates='room')

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
    module_slug = lobby_db.Column(lobby_db.String(50), primary_key=False)
    ope_namespace = lobby_db.Column(lobby_db.String(50), primary_key=False)
    agent = lobby_db.Column(lobby_db.String(30), primary_key=False)
    socket_id = lobby_db.Column(lobby_db.String(50))
    start_time = lobby_db.Column(lobby_db.DateTime(timezone=False), server_default=func.now())
    room_name = lobby_db.Column(lobby_db.String(50))
    room_id = lobby_db.Column(lobby_db.Integer, lobby_db.ForeignKey('room.id'), nullable=True)
    session_url_notified = lobby_db.Column(lobby_db.Boolean, primary_key=False)
    room = lobby_db.relationship('Room', back_populates='users')

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

@app.route('/sail_lobby/<user_id>', methods=['GET', 'POST'])
def sail_lobby(user_id):
    print("sail_lobby: received user_id " + str(user_id), flush=True)
    return render_template('lobby.html', user_id=user_id)

@app.route('/getJupyterlabUrl', methods=['POST'])
def getJupyterlabUrl():
    print("getJupyterlabUrl: enter", flush=True)
    data = request.get_json()
    user_id = data.get('userId')
    name = data.get('name')
    email = data.get('email')
    password = data.get('password')
    entity_id = data.get('entityId')
    print(f"getJupyterlabUrl -- user_id: {user_id} -- name: {name} -- email: {email}", flush=True)
    user_info = {'user_id': str(user_id), 'socket_id': str(socket_id), 'name': str(name), 'email': str(email),
                 'password': str(password), 'entity_id': str(entity_id), 'agent': str(agent)}
    user_queue.put(user_info)
    url = lobby_url_prefix + user_id
    response_data = {
        "url": url
    }
    return jsonify(response_data), 200
    # return render_template('lobby.html', user_id=user_id)

@socketio.on('sail_lobby_connect')
def sail_lobby_connect(user_data):
    socket_id = request.sid
    user_id = user_data.get('userId')
    print(f"sail_lobby_connect -- socket_id: {socket_id} -- user_id:  {user_id}", flush=True)
    user_info = {'user_id': str(user_id), 'socket_id': str(socket_id)}
    user_queue.put(user_info)
    print("sail_lobby_connect - user_queue length: " + str(user_queue.qsize()), flush=True)
    emit('response_event', "Data received successfully")

@socketio.on('user_connect')
def process_user_connect(user_data):
    socket_id = request.sid
    user_id = user_data.get('userId')
    name = user_data.get('name')
    email = user_data.get('email')
    password = user_data.get('password')
    entity_id = user_data.get('entityId')
    agent = user_data.get('agent')
    print(f"Client connected with socket_id: {socket_id} -- user_id:  {user_id} -- email: {email}", flush=True)
    user_info = {'user_id': str(user_id), 'socket_id': str(socket_id), 'name': str(name), 'email': str(email),
                 'password': str(password), 'entity_id': str(entity_id), 'agent': str(agent)}
    user_queue.put(user_info)
    print("Login - user_queue length: " + str(user_queue.qsize()), flush=True)
    emit('response_event', "Data received successfully")


@socketio.on('disconnect')
def process_disconnect():
    socket_id = request.sid
    print(f"Client DISCONNECTED -- socket_id: {socket_id}", flush=True)


def shutdown_server():
    user_queue.put(None)
    consumer_thread.join()


def reassign_room(user, room):
    with app.app_context():
        user_message = str(user.user_id) + ", return to your room link: " + str(room.session_url)
        print("reassign_room message: " + user_message, flush=True)
        print("reassign_room socket_id: " + str(user.socket_id), flush=True)
        socketio.emit('update_event', {'message': user_message}, room=user.socket_id)


def assign_rooms():
    global unassigned_users
    num_unassigned_users = len(unassigned_users)
    if num_unassigned_users > 0:

        # Fill rooms that have less than the target number of users
        if fillRoomsUnderTarget:
            assign_rooms_under_n_users(targetUsersPerRoom)

        # Fill rooms with the target number of users
        if num_unassigned_users >= targetUsersPerRoom:
            assign_new_rooms(targetUsersPerRoom)

        # Check for any users waiting long enough that they should get a suboptimal assignment
        users_due_for_suboptimal = get_users_due_for_suboptimal()
        num_users_due_for_suboptimal = len(users_due_for_suboptimal)

        # Overfill rooms if there are not enough unassigned users to create a new room
        if (num_unassigned_users > 0) and overFillRooms:
            if (num_users_due_for_suboptimal > 0) and (len(unassigned_users) < minUsersPerRoom):
                assign_rooms_under_n_users(maxUsersPerRoom)

        # Create a new room, even with less than the target number of users, if there are enough unassigned users
        if num_unassigned_users > 0:
            if (num_users_due_for_suboptimal > 0) and (num_unassigned_users >= minUsersPerRoom):
                assign_new_room(num_unassigned_users)

        prune_users()       # tell users who have been waiting too long to come back later


def assign_rooms_under_n_users(n_users):
    # Assuming
    #   -- assign to rooms that are most-under n-users first
    #   -- fill one room to n-users before assigning to next room
    global unassigned_users
    available_rooms_under_n_users = []
    if len(unassigned_users) > 0:
        available_rooms_under_n_users = get_sorted_available_rooms(n_users)
    i = 0
    is_room_new = False
    with app.app_context():
        while (i < len(available_rooms_under_n_users)) and (len(unassigned_users) > 0):
            assign_up_to_n_users(available_rooms_under_n_users[i], n_users, is_room_new)
            i += 1


def get_users_due_for_suboptimal():
    global unassigned_users
    users_due_for_suboptimal = []
    i = 0
    if len(unassigned_users) > targetUsersPerRoom:   # Don't assign suboptimally if there are now target num of users
        print("get_users_due_for_suboptimal(): now there are enough users for the target", flush=True)
        return users_due_for_suboptimal
    with app.app_context():
        while i < len(unassigned_users):
            user = unassigned_users[i]
            time_diff = time.time() - user.start_time.timestamp()
            print("Time diff for unassignedUser(" + user.user_id + "): " + str(time_diff), flush=True)
            if time_diff > maxWaitTimeForSubOptimalAssignment:
                users_due_for_suboptimal.append(unassigned_users[i])
            i += 1
    return users_due_for_suboptimal


def assign_up_to_n_users(room, num_users, is_room_new):
    print("assign_up_to_n_users - incoming num_users: " + str(num_users))
    global unassigned_users
    while (len(room.users) < num_users) and (len(unassigned_users) > 0):
        user = unassigned_users[0]
        assign_room(user, room, is_room_new)
        unassigned_users.remove(user)
    print("assign_up_to_n_users - final room.num_users: " + str(room.num_users))
    if not is_room_new:
        request_session(room)


def assign_new_rooms(num_users_per_room):
    global unassigned_users
    while len(unassigned_users) > num_users_per_room:
        assign_new_room(num_users_per_room)


def request_session(room):
    global generalRequestPrefix, sessionRequestPath, moduleSlug, opeBotNamespace, opeBotNamespace, opeBotUsername
    request_url = generalRequestPrefix + "/" + sessionRequestPath + "/" + moduleSlug + "/" + room.room_name
    current_time = datetime.now()
    with app.app_context():
        user_list = []
        i = 0
        while i < room.num_users:
            user = room.users[i]
            user_element = {'namespace': moduleSlug, 'name': email_to_dns(user.email)}
            user_list.append(user_element)
            i += 1
        data = {
            "spec": {
                'startTime': current_time.strftime("%Y-%m-%dT%H:%M:%S-%z:%Z"),
                'moduleSlug': moduleSlug,
                'opeBotRef': {
                    'namespace': opeBotNamespace,
                    'name': opeBotName
                },
                'opeUsersRef': user_list
            }
        }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(request_url, data=json.dumps(data), headers=headers)

    # {Change the following to continue posting requests for awhile until successful?}
    if response.status_code == 200:
        response_data = response.json()
        result = response_data.get('result')
        print("request_session: POST successful")
        print("request_session, result: " + str(result))
        # return str(result)
    else:
        print("request_session: POST failed -- response code " + str(response.status_code))
        # return None


def request_user(user, room):
    global userRequestPath, moduleSlug
    with app.app_context():
        request_url = generalRequestPrefix + "/" + userRequestPath + "/" + moduleSlug + "/" + email_to_dns(user.email)
        data = {
            'enableMatch': False,
            'isBot': False,
            'stressTest': False,
            'name': user.name,
            'email': user.email,
            'password': user.password,
            'moduleSlug': user.module_slug,
            'opeSessionRef': [
                {
                    'namespace': room.module_slug,
                    'name': room.room_name
                }
            ]
        }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(request_url, data=json.dumps(data), headers=headers)

    if response.status_code == 200:
        response_data = response.json()
        result = response_data.get('result')
        print("request_user: POST successful")
        print("request_user, result: " + str(result))
        print("request_user: session POST succeeded")
        # return str(result)
    else:
        print("request_user: session POST failed -- response code " + str(response.status_code))
        # return None


def request_room_status(room):
    global generalRequestPrefix, sessionRequestPath, moduleSlug
    with app.app_context():
        request_url = generalRequestPrefix + "/" + sessionReadinessPath + "/" + moduleSlug + "/" + room.room_name
        # with app.app_context():
        # data = {
        # }
        # headers = {'Content-Type': 'application/json'}
        # response = requests.get(request_url, data=json.dumps(data), headers=headers)
    response = requests.get(request_url)

    if response.status_code == 200:
        response_data = response.json()
        session_url = response_data.get('session')
        print("request_room_status succeeded - URL: " + session_url)
        return session_url
    else:
        print("request_room_status failed -- response code " + str(response.status_code))
        return None


def email_to_dns(email):
    email1 = email.replace('@', '-at-')
    email2 = email1.replace('.', '-')
    # print("email_to_dns - original: " + email + "  -- dns: " + email2)
    return email2


def assign_new_room(num_users):
    global nextRoomNum, session, sessionUpdated, rooms

    # {This will be replaced by the result of request_session(). }
    nextRoomNum += 1
    room_name = roomPrefix + str(nextRoomNum)
    # request_session(room_name, num_users)
    is_room_new = True

    with app.app_context():
        room = Room(room_name=room_name, session_url=None, num_users=0)
        session.add(room)
        session.commit()
        session = lobby_db.session
        assign_up_to_n_users(room, num_users, is_room_new)
        request_session(room)

    print("assign_new_room - room.num_users: " + str(room.num_users))
    # check if DB is updated
    # if DB is updated, can get users by searching User by room name

    # for user.id in room.users:
    #     user = session.query(User).filter_by(user.id).first()
    #     request_user(user, room)

    # for user in room.users:
    #     print("   " + user.user_id, flush=True)


def assign_room(user, room, is_room_new):
    global unassigned_users, session, sessionUpdated
    user.room_name = room.room_name
    room.users.append(user)
    room.num_users += 1
    if room.room_name != "waiting_room":
        waiting_room = Room.query.filter_by(room_name="waiting_room").first()
        waiting_room.num_users -= 1
        session.add(waiting_room)
    if not is_room_new:
        request_user(user, room)
        tell_users_session_url(room)
    session.add(room)
    # sessionUpdated = True
    session.commit()
    session = lobby_db.session
    # if room.room_name != "waiting_room":
    #     user_message = str(user.user_id) + ", here's your room link: " + str(room.session_url)
    #     print("assign_room: socket_id: " + user.socket_id + "    message: " + user_message, flush=True)
    #     socketio.emit('update_event', {'message': user_message, 'url': room.session_url}, room=user.socket_id)


# 1. Sort primarily by number of users (ascending), then secondarily by start_time (ascending)
#    to prioritize rooms primarily by the least users and secondarily by the oldest start time.
# 2. Select rooms that are not tool old to add new users.
# 3. Strip waiting_room from results.
# 4. Strip rooms with >= max_users from the results.

def get_sorted_available_rooms(max_users):
    room_list = []
    with app.app_context():
        sorted_rooms = Room.query.order_by(Room.num_users.asc(), Room.start_time.asc()).all()
        for room in sorted_rooms:
            if room.room_name != "waiting_room":
                print("   " + room.room_name + "  -  users: " + (str(len(room.users))))
        current_time = time.time()
        for room in sorted_rooms:
            time_diff = current_time - room.start_time.timestamp()
            if (time_diff < maxRoomAgeForNewUsers) and (room.room_name != "waiting_room"):
                if len(room.users) < max_users:
                    room_list.append(room)
        print("get_sorted_available_rooms:")
        if len(room_list) > 0:
            for room in room_list:
                print("   " + room.room_name + "  -  users: " + (str(len(room.users))))
        else:
            print("   no rooms available")
    return room_list


def prune_users():
    global unassigned_users, session, sessionUpdated
    for user in unassigned_users:
        with app.app_context():
            if (time.time() - user.start_time.timestamp()) >= maxWaitTimeUntilGiveUp:
                user_message = str(user.user_id) + \
                    ": I'm sorry. There are not enough other users logged in right now to start a session. \
                    Please try again later."
                print("prune_users: socket_id: " + user.socket_id + "    message: " + user_message, flush=True)
                socketio.emit('update_event', {'message': user_message}, room=user.socket_id)
                unassigned_users.remove(user)
                User.query.filter(User.id == user.id).delete()
                waiting_room = Room.query.filter_by(room_name="waiting_room").first()
                waiting_room.num_users -= 1
                # session.commit()
                # sessionUpdated = True
                session.commit()
                session = lobby_db.session
    return unassigned_users


def print_room_assignments():
    with app.app_context():
        created_rooms = Room.query.all()
        if len(created_rooms) > 0:
            for room in created_rooms:
                print("Room " + str(room.room_name) + " - num_users: " + str(room.num_users), flush=True)
                for user in room.users:
                    print("   " + user.user_id, flush=True)
        else:
            print("No rooms yet", flush=True)


def print_users():
    with app.app_context():
        users = User.query.all()
        if len(users) > 0:
            for user in users:
                print("User: " + str(user.user_id), flush=True)
        else:
            print("No rooms yet", flush=True)


def tell_users_session_url(room):
    global sessionUpdated, session
    if room.session_url is not None:
        users = room.users
        for user in users:
            if not user.session_url_notified:
                user_message = str(user.user_id) + ", here's your room link: " + str(room.session_url)
                print("tell_users_session_url: socket_id: " + user.socket_id + "    message: " + user_message,
                      flush=True)
                socketio.emit('update_event', {'message': user_message, 'url': room.session_url}, room=user.socket_id)
                user.session_url_notified = True
                session.add(user)
                # sessionUpdated = True
                session.commit()
                session = lobby_db.session


def check_for_new_sessions():
    global sessionUpdated, session, rooms
    with app.app_context():
        rooms = Room.query.all()
        for room in rooms:
            if (room.room_name != "waiting_room") and (room.session_url is None):
                session_url = request_room_status(room)
                if session_url is not None:
                    print("check_for_new_sessions - session_url for room " + room.room_name +
                          " is " + str(session_url))
                else:
                    print("check_for_new_sessions - session_url for room " + room.room_name +
                          " is None")

                # TEMPORARILY FAKING SESSION_URL RESPONSE
                if session_url is None:
                    print("check_for_new_sessions - *** creating fake session URL ***")
                    session_url = fake_session_url

                if session_url is not None:
                    room.session_url = session_url
                    tell_users_session_url(room)
                    session.add(room)
                    session.commit()
                    session = lobby_db.session


def assigner():
    global lobby_db, lobby_initialized, unassigned_users, session, assigner_sleep_time, moduleSlug, sessionUpdated
    if not lobby_initialized:
        with app.app_context():
            session = lobby_db.session
            lobby_db.drop_all()
            lobby_db.create_all()
            waiting_room = Room(room_name="waiting_room", session_url=None, num_users=0)
            print("assigner - Created waiting_room - room_name: " + waiting_room.room_name, flush=True)
            session.add(waiting_room)
            session.commit()
            # session = None
            session = lobby_db.session
            lobby_initialized = True
    while True:
        # if session is None:
        #     # session= sessionmaker(bind=lobby_db.engine)
        #     session = lobby_db.session
        #     sessionUpdated = False
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
                    user = User.query.filter_by(user_id=user_id).first()

                    # If user has previously logged in
                    if user is not None:
                        print("assigner: user " + str(user_id) + " has previously logged in. NEW socket_id: "
                              + str(socket_id), flush=True)
                        room_id = user.room_id
                        room = Room.query.get(room_id)
                        room_name = room.room_name
                        print("assigner; user is not none - room_name: " + room_name, flush=True)
                        print("                   start_time: " + str(user.start_time) + "  room: " +
                              str(user.room_id) + "  room_name: " + room.room_name + "  OLD socket_id: " +
                              user.socket_id + "  name: " + user.name, flush=True)
                        room_id = user.room_id
                        # update_user_socket(user_id, socket_id)
                        user.socket_id = socket_id
                        session.add(user)
                        # sessionUpdated = True
                        session.commit()
                        session = lobby_db.session
                        print("assigner: user " + str(user_id) + " UPDATED socket_id: " + str(user.socket_id),
                              flush=True)

                        # If user already assigned to a room, reassign
                        if room_id is not None and room_name != "waiting_room":
                            print("assigner: user " + str(user_id) + " already has room " + room.room_name, flush=True)
                            reassign_room(user, room)

                        # else previously logged-in user should already be in the unassigned_users list
                        #      -- but double-checking as a failsafe
                        elif user not in unassigned_users:
                            print("assigner: adding user " + str(user_id) + " to unassigned_users")
                            unassigned_users.append(user)

                    # This is a new user
                    else:
                        print("assigner: user " + str(user_id) + " is a new user")
                        waiting_room = Room.query.filter_by(room_name="waiting_room").first()
                        user = User(user_id=user_id, name=name, email=email, password=password,
                                    entity_id=entity_id, agent=agent, socket_id=socket_id, room=waiting_room,
                                    room_name=waiting_room.room_name, module_slug=moduleSlug,
                                    ope_namespace=moduleSlug, session_url_notified=False)
                        session.add(user)
                        # sessionUpdated = True
                        session.commit()
                        session = lobby_db.session
                        is_room_new = False
                        assign_room(user, waiting_room, is_room_new)
                        unassigned_users.append(user)
                        print("assigner - user " + user.user_id + " start_time: " + str(user.start_time) +
                              "   room_name: " + waiting_room.room_name + "   socket_id: " + user.socket_id +
                              "   name: " + user.name, flush=True)
                        print("assigner - time.time(): " + str(time.time()), flush=True)

        with app.app_context():
            unassigned_users = User.query.filter_by(room_name="waiting_room").order_by(User.start_time.asc()).all()

        if len(unassigned_users) > 0:
            print("assigner -- unassigned_users: ", flush=True)
            for i in range(len(unassigned_users)):
                print("  user.id: " + str(unassigned_users[i]), flush=True)
            assign_rooms()
            # print("\n\n")
            print_room_assignments()

        check_for_new_sessions()

        time.sleep(assigner_sleep_time)


consumer_thread = threading.Thread(target=assigner)
consumer_thread.start()


if __name__ == '__main__':
    session = lobby_db.session
    socketio.run(app, threaded=True, port=5000)
    shutdown_server()
