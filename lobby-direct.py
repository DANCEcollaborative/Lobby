import os
import time
from datetime import datetime
import pytz
import threading
import queue
import json
import requests
# from enum import Enum
from flask import Flask, request
# from flask import Flask, request, render_template, jsonify
# from flask import Flask, request, render_template
# from flask_socketio import SocketIO, emit
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func
# from sqlalchemy.orm import sessionmaker

# Queue to pass incoming users
user_queue = queue.Queue()
thread_lock = threading.Lock()
condition = threading.Condition()

targetUsersPerRoom = 4
minUsersPerRoom = 2
maxUsersPerRoom = 5
maxWaitTimeForSubOptimalAssignment = 10        # seconds
maxWaitTimeUntilGiveUp = 70                    # seconds    >>> UPDATE THIS <<<
maxRoomAgeForNewUsers = 60                     # seconds    >>> UPDATE THIS <<<
fillRoomsUnderTarget = True
overFillRooms = True
lobby_initialized = False
assigner_sleep_time = 1     # sleep time in seconds between assigner iterations
unassigned_users = []       # Users with no room assignment
users_to_notify = []        # Users with assigned URL

# I THINK THE FOLLOWING WILL BE OBSOLETE
MAX_GROUP_SIZE = 4
MAX_WAIT_TIME = 180

lobby_url_prefix = 'http://bazaar.lti.cs.cmu.edu:5000/sail_lobby/'
generalRequestPrefix = 'https://ope.sailplatform.org/api/v1'
activityUrlLinkPrefix = '<a href="'
activityUrlLinkSuffix = '">OPE Session</a>'
sessionOnlyRequestPath = 'opesessions'
sessionPlusUsersRequestPath = 'scheduleSession'
userRequestPath = 'opeusers'
sessionReadinessPath = 'sessionReadiness'
roomPrefix = "room"
nextRoomNum = 200
nextThreadNum = 0
threadMapping = {}
eventMapping = {}
# rooms = {}
moduleSlug = 'ope-learn-practice-p032vbfd'
namespace = 'default'
opeBotName = 'bazaar-lti-at-cs-cmu-edu'
localTimezone = pytz.timezone('America/New_York')

opeBotUsername = 'bazaar-lti-cs-cmu-edu'  # UPDATE THIS ???

basedir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] =\
        'postgresql://postgres:testpwd@lobby_db:5432/lobby_db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_EXPIRE_ON_COMMIT'] = False
lobby_db = SQLAlchemy(app)
session = None


class Room(lobby_db.Model):
    __tablename__ = 'room'
    id = lobby_db.Column(lobby_db.Integer, primary_key=True)
    room_name = lobby_db.Column(lobby_db.String(50))
    activity_url = lobby_db.Column(lobby_db.String(200))
    module_slug = moduleSlug
    bot_namespace = namespace
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
    password = lobby_db.Column(lobby_db.String(100), primary_key=False)
    entity_id = lobby_db.Column(lobby_db.String(40), primary_key=False)
    module_slug = lobby_db.Column(lobby_db.String(50), primary_key=False)
    start_time = lobby_db.Column(lobby_db.DateTime(timezone=False), server_default=func.now())
    room_name = lobby_db.Column(lobby_db.String(50))
    room_id = lobby_db.Column(lobby_db.Integer, lobby_db.ForeignKey('room.id'), nullable=True)
    activity_URL = lobby_db.Column(lobby_db.String(100), primary_key=False)
    activity_url_notified = lobby_db.Column(lobby_db.Boolean, primary_key=False)
    ope_namespace = namespace
    agent = lobby_db.Column(lobby_db.String(30), primary_key=False)
    thread_name = lobby_db.Column(lobby_db.String(12), primary_key=False)
    event_name = lobby_db.Column(lobby_db.String(12), primary_key=False)
    room = lobby_db.relationship('Room', back_populates='users')

    def __repr__(self):
        return f'<User {self.user_id}>'
#
#
# class NamedEvent(threading.Event):
#     def __init__(self, name):
#         super().__init__()
#         self.name = name


@app.route('/getJupyterlabUrl', methods=['POST'])
def getJupyterlabUrl():
    global user_queue, session, nextThreadNum, threadMapping, eventMapping
    print("lobby-direct.py, getJupyterlabUrl: enter", flush=True)
    nextThreadNum += 1
    event_name = "event" + str(nextThreadNum)
    thread_name = "thread" + str(nextThreadNum)
    event = threading.Event()
    eventMapping[event_name] = event
    with thread_lock:
        current_user = threading.current_thread()
        threadMapping[thread_name] = current_user
        current_user.event = event
        data = json.loads(request.data.decode('utf-8'))
        name = data.get('name')
        email = data.get('email')
        password = data.get('password')
        entity_id = data.get('entityId')
        user_id = email_to_dns(email)
        print(f"getJupyterlabUrl -- user_id: {user_id} -- name: {name} -- email: {email}",
              flush=True)
        with app.app_context():
            user = User.query.filter_by(user_id=user_id).first()

            # If user is not new
            if user is not None:
                user_info = {'user_id': str(user_id), 'name': str(name), 'email': str(email),
                             'password': str(password), 'entity_id': str(entity_id)}
                duplicate_user = is_duplicate_user(user_info, user)

                # If old non-duplicate user, delete and start over
                if not duplicate_user:
                    print("assigner: user " + str(user_id) + " is an old non-duplicate user -- starting over")
                    session.delete(user)
                    session.commit()
                    session = lobby_db.session
                    if user in unassigned_users:
                        unassigned_users.remove(user)
                    user = User(user_id=user_id, name=name, email=email, password=password,
                                entity_id=entity_id, ope_namespace=namespace, module_slug=moduleSlug,
                                activity_url_notified=False, thread_name=thread_name, event_name=event_name)
                    session.add(user)
                    session.commit()
                    session = lobby_db.session

                # If duplicate user, they'll need a URL notification and maybe a room assignment
                if duplicate_user:
                    user.activity_url_notified = False
                    session.add(user)
                    session.commit()
                    session = lobby_db.session

            # New user
            if user is None:
                print("getJupyterlabUrl: user " + str(user_id) + " is a new user")
                user = User(user_id=user_id, name=name, email=email, password=password,
                            entity_id=entity_id, ope_namespace=namespace, module_slug=moduleSlug,
                            activity_url_notified=False, thread_name=thread_name, event_name=event_name)
                session.add(user)
                session.commit()
                session = lobby_db.session

        user_queue.put(current_user, user_id)

    with condition:
        condition.notify_all()

    event.wait()

    if current_user.code == 200:
        return current_user.url
    else:
        return current_user.code


def request_session_then_users(room):
    global generalRequestPrefix, sessionOnlyRequestPath, moduleSlug, namespace, opeBotUsername, localTimezone
    request_url = generalRequestPrefix + "/" + sessionOnlyRequestPath + "/" + namespace + "/" + room.room_name
    print("request_session_then_users -- request_url: " + request_url, flush=True)
    with app.app_context():
        user_list = []
        i = 0
        while i < room.num_users:
            user = room.users[i]
            user_element = {'namespace': namespace, 'name': email_to_dns(user.email)}
            user_list.append(user_element)
            i += 1
        data = {
            "spec": {
                "startTime": datetime.now(localTimezone).replace(microsecond=0).isoformat(),
                "moduleSlug": moduleSlug,
                "opeBotRef": {
                    "namespace": namespace,
                    "name": opeBotName
                },
                "opeUsersRef": user_list
            }
        }
    print("request_session_then_users -- data as string: " + str(data), flush=True)
    headers = {'Content-Type': 'application/json'}
    response = requests.post(request_url, data=json.dumps(data), headers=headers)

    if response.status_code == 200:
        print("request_session_then_users: POST successful", flush=True)
        with app.app_context():
            for user in room.users:
                request_user(user, room)
    else:
        print("request_session_then_users: POST failed -- response code " + str(response.status_code))


def request_session_plus_users(room):
    global generalRequestPrefix, sessionPlusUsersRequestPath, moduleSlug, namespace, opeBotUsername, localTimezone
    request_url = generalRequestPrefix + "/" + sessionPlusUsersRequestPath
    print("request_session_plus_users -- request_url: " + request_url, flush=True)
    with app.app_context():
        user_list = []
        i = 0
        while i < room.num_users:
            user = room.users[i]
            user_element = {"name": user.name, "email": user.email, "password": user.password}
            user_list.append(user_element)
            i += 1
        data = {
            "sessions": [
                {
                    "users": user_list,
                    "moduleSlug": moduleSlug,
                    "sessionName": room.room_name,
                    "startTime": datetime.now(localTimezone).replace(microsecond=0).isoformat(),
                }
            ]
        }
    print("request_session_plus_users -- data as string: " + str(data), flush=True)
    headers = {'Content-Type': 'application/json'}
    response = requests.post(request_url, data=json.dumps(data), headers=headers)

    if response.status_code == 200:
        print("request_session_plus_users: POST successful", flush=True)
    else:
        print("request_session_plus_users: POST failed -- response code " + str(response.status_code))


def request_user(user, room):
    global userRequestPath, namespace, moduleSlug
    with app.app_context():
        request_url = generalRequestPrefix + "/" + userRequestPath + "/" + namespace + "/" + email_to_dns(user.email)
        print("request_user -- request_url: " + request_url, flush=True)
        data = {
            'enableMatch': False,
            'isBot': False,
            'stressTest': False,
            'name': user.name,
            'email': user.email,
            'password': user.password,
            'moduleSlug': moduleSlug,
            'opeSessionRef': [
                {
                    'namespace': namespace,
                    'name': room.room_name
                }
            ]
        }
    print("request_user -- data as string: " + str(data), flush=True)
    headers = {'Content-Type': 'application/json'}
    response = requests.post(request_url, data=json.dumps(data), headers=headers)

    if response.status_code == 200:
        # response_data = response.json()
        # result = response_data.get('result')
        # print("request_user: POST successful")
        # print("request_user, result: " + str(result))
        print("request_user: POST succeeded")
        # return str(result)
    else:
        print("request_user: POST failed -- response code " + str(response.status_code))
        # return None


def request_room_status(room):
    global generalRequestPrefix, sessionReadinessPath, moduleSlug
    with app.app_context():
        request_url = generalRequestPrefix + "/" + sessionReadinessPath + "/" + namespace + "/" + moduleSlug + \
                      "-" + room.room_name
        print("request_room_status -- request_url: " + request_url, flush=True)
        # with app.app_context():
        # data = {
        # }
        # headers = {'Content-Type': 'application/json'}
        # response = requests.get(request_url, data=json.dumps(data), headers=headers)
    response = requests.get(request_url)

    if response.status_code == 200:
        print("request_room_status succeeded -- response code " + str(response.status_code))
        response_data = response.text
        return response_data
    else:
        print("request_room_status failed -- response code " + str(response.status_code))
        return None


def request_room_status_without_module_slug(room):
    global generalRequestPrefix, sessionReadinessPath, moduleSlug
    with app.app_context():
        request_url = generalRequestPrefix + "/" + sessionReadinessPath + "/" + namespace + "/" + room.room_name
        print("request_room_status_without_module_slug -- request_url: " + request_url, flush=True)
        # with app.app_context():
        # data = {
        # }
        # headers = {'Content-Type': 'application/json'}
        # response = requests.get(request_url, data=json.dumps(data), headers=headers)
    response = requests.get(request_url)

    if response.status_code == 200:
        response_data = response.text
        return response_data
    else:
        print("request_room_status_without_module_slug failed -- response code " + str(response.status_code))
        return None


def check_for_new_activity_urls():
    global session
    with app.app_context():
        rooms = Room.query.all()
        for room in rooms:
            # activity_url = None
            if room.room_name != "waiting_room" and room.activity_url is None:
                wait_time = time.time() - room.start_time.timestamp()
                print("check_for_new_activity_urls -- room " + room.room_name + " wait time: " + str(wait_time),
                      flush=True)
                if wait_time >= maxWaitTimeUntilGiveUp:
                    prune_room(room)
                else:
                    activity_url = request_room_status(room)
                    if activity_url is not None:
                        print("check_for_new_activity_urls - activity_url for room " + room.room_name +
                              " is " + str(activity_url))
                        room.activity_url = activity_url
                        assign_users_activity_url(room)
                        session.add(room)
                        session.commit()
                        session = lobby_db.session
                    else:
                        print("check_for_new_activity_urls - activity_url for room " + room.room_name +
                              " is None")


def assign_users_activity_url(room):
    global session, users_to_notify, threadMapping
    with app.app_context():
        activity_url = room.activity_url
        if activity_url is not None:
            users = room.users
            # activity_link = activityUrlLinkPrefix + room.activity_url + activityUrlLinkSuffix
            for user in users:
                user.activity_url = activity_url
                print("assign_users_activity_url: user: " + user.name + "  --   URL: " + user.activity_url, flush=True)
                session.add(user)
                session.commit()
                session = lobby_db.session
                user_thread = threadMapping[user.thread_name]
                user_thread.code = 200
                user_thread.url = activity_url
                user_event = eventMapping[user.event_name]
                users_to_notify.append(user_event)


def email_to_dns(email):
    email1 = email.replace('@', '-at-')
    email2 = email1.replace('.', '-')
    # print("email_to_dns - original: " + email + "  -- dns: " + email2)
    return email2


def is_duplicate_user(user_info, user):
    with app.app_context():
        name = user_info['name']
        email = user_info['email']
        password = user_info['password']
        entity_id = user_info['entity_id']
        if name != user.name:
            return False
        if email != user.email:
            return False
        if password != user.password:
            return False
        if entity_id != user.entity_id:
            return False
        return True


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


def assign_up_to_n_users(room, num_users, is_room_new):
    print("assign_up_to_n_users - incoming num_users: " + str(num_users))
    global unassigned_users
    while (len(room.users) < num_users) and (len(unassigned_users) > 0):
        user = unassigned_users[0]
        assign_room(user, room, is_room_new)
        unassigned_users.remove(user)
    print("assign_up_to_n_users - final room.num_users: " + str(room.num_users))


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


def assign_new_rooms(num_users_per_room):
    global unassigned_users
    while len(unassigned_users) > num_users_per_room:
        assign_new_room(num_users_per_room)


def assign_new_room(num_users):
    global nextRoomNum, session

    # {This will be replaced by the result of request_session(). }
    nextRoomNum += 1
    room_name = roomPrefix + str(nextRoomNum)
    # request_session(room_name, num_users)
    is_room_new = True

    with app.app_context():
        room = Room(room_name=room_name, activity_url=None, num_users=0)
        session.add(room)
        session.commit()
        session = lobby_db.session
        assign_up_to_n_users(room, num_users, is_room_new)
        request_session_plus_users(room)

    print("assign_new_room - room.num_users: " + str(room.num_users))


def assign_room(user, room, is_room_new):
    global unassigned_users, session
    user.room_name = room.room_name
    room.users.append(user)
    room.num_users += 1
    if not is_room_new:
        request_user(user, room)
        # tell_users_activity_url(room)
    session.add(room)
    session.commit()
    session = lobby_db.session


def prune_users():
    global unassigned_users, session, threadMapping, users_to_notify
    for user in unassigned_users:
        with app.app_context():
            if (time.time() - user.start_time.timestamp()) >= maxWaitTimeUntilGiveUp:
                user_message = str(user.user_id) + \
                    ": I'm sorry. There are not enough other users logged in right now to start a session. \
                    Please try again later."
                print("prune_users: socket_id: " + user.socket_id + "    message: " + user_message, flush=True)
                unassigned_users.remove(user)
                User.query.filter(User.id == user.id).delete()
                session.commit()
                session = lobby_db.session
                user_thread = threadMapping[user.thread_name]
                user_thread.code = 408
                user_event = eventMapping[user.event_name]
                users_to_notify.append(user_event)

    return unassigned_users


def prune_room(room):
    global session, users_to_notify
    with app.app_context():
        for user in room.users:
            user_message = str(user.name) + \
                ", I'm sorry. There is no session available right now. Please try again later."
            print("prune_room: socket_id: " + user.socket_id + "    message: " + user_message, flush=True)
            user_thread = threadMapping[user.thread_name]
            user_thread.code = 404
            user_event = eventMapping[user.event_name]
            users_to_notify.append(user_event)
            User.query.filter(User.id == user.id).delete()
            session.commit()
            session = lobby_db.session
        Room.query.filter(Room.id == room.id).delete()
        session.commit()
        session = lobby_db.session


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


def assigner():
    global nextRoomNum, lobby_initialized, session, unassigned_users, users_to_notify, eventMapping

    # Initialize Lobby
    if not lobby_initialized:
        with app.app_context():
            session = lobby_db.session
            lobby_db.drop_all()
            lobby_db.create_all()
            waiting_room = Room(room_name="waiting_room", activity_url=None, num_users=0)
            print("assigner - Created waiting_room - room_name: " + waiting_room.room_name, flush=True)
            session.add(waiting_room)
            session.commit()
            # session = None
            session = lobby_db.session
            lobby_initialized = True

    # Repeat continuously while Lobby is running
    while True:
        users_to_notify = []        # Clear any previously notified users from list to notify
        with condition:
            # Wait for a user to join
            condition.wait()

            # current_time = time.time()

            # Get users in the queue
            while not user_queue.empty():
                # current_user, user_id = user_queue.queue[0]
                current_user, user_id = user_queue.get()
                with app.app_context():
                    user = User.query.filter_by(user_id=user_id).first()
                    user.activity_url_notified = False  # User needs at least a URL returned

                    if user.room_id is None:
                        waiting_room = Room.query.filter_by(room_name="waiting_room").first()
                        user.room = waiting_room
                        user.room_name = waiting_room.room_name
                        session.add(user)
                        session.commit()
                        session = lobby_db.session
                        print("assigner: adding user " + str(user_id) + " to unassigned_users")
                        unassigned_users.append(user)

            # Get current list of unassigned users
            with app.app_context():
                unassigned_users = User.query.filter_by(room_name="waiting_room").order_by(
                    User.start_time.asc()).all()

            # Assign users to rooms as they become available
            if len(unassigned_users) > 0:
                print("assigner -- unassigned_users: ", flush=True)
                for i in range(len(unassigned_users)):
                    print("  user.id: " + str(unassigned_users[i]), flush=True)
                assign_rooms()
                print_room_assignments()

            # Check for new activity URLs as they become available
            check_for_new_activity_urls()

            # Wake up all users in the group with new activity URLs or negative status
            for event in users_to_notify:
                event.set()

            time.sleep(assigner_sleep_time)


if __name__ == '__main__':
    session = lobby_db.session
    threading.Thread(target=assigner, daemon=True).start()
    app.run(debug=True)
