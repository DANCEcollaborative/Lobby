import os
import time
from datetime import datetime
import pytz
import threading
import queue
import json
import requests
from requests.exceptions import RequestException
from flask import Flask, request, make_response, Response
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func
from urllib.parse import unquote
import io
from contextlib import redirect_stdout

# ROOM ALLOCATION CONSTANTS
TARGET_USERS_PER_ROOM = 3
MIN_USERS_PER_ROOM = 1
MAX_USERS_PER_ROOM = 4
FILL_ROOMS_UNDER_TARGET = True
OVERFILL_ROOMS = True

# TIME CONSTANTS -- all in seconds
MAX_WAIT_TIME_FOR_SUBOPTIMAL_ASSIGNMENT = 5
MAX_WAIT_TIME_UNTIL_GIVE_UP = 5 * 60
MAX_ROOM_AGE_FOR_NEW_USERS = 10 * 60
ASSIGNER_SLEEP_TIME = 1
ELAPSED_TIME_UNTIL_USER_DELETION = 120 * 60
ELAPSED_TIME_UNTIL_ROOM_DELETION = 130 * 60
CHECK_FOR_USER_DELETION_WAIT_TIME = 1 * 60
CHECK_FOR_ROOM_DELETION_WAIT_TIME = 2 * 60

# KUBERNETES- and HTTP-RELATED CONSTANTS
OPE_BOT_NAME = 'bazaar-lti-at-cs-cmu-edu'
OPE_BOT_USERNAME = 'bazaar-lti-cs-cmu-edu'
LOCAL_TIME_ZONE = pytz.timezone('America/New_York')
LOBBY_URL_PREFIX = 'http://bazaar.lti.cs.cmu.edu:5000/sail_lobby/'
REQUEST_PREFIX = 'https://ope.sailplatform.org/api/v1'
ACTIVITY_URL_LINK_PREFIX = '<a href="'
ACTIVITY_URL_LINK_SUFFIX = '">OPE Session</a>'
SESSION_ONLY_REQUEST_PATH = 'opesessions'
SESSION_PLUS_USERS_REQUEST_PATH = 'scheduleSession'
USER_REQUEST_PATH = 'opeusers'
SESSION_READINESS_PATH = 'sessionReadiness'
MODULE_SLUG = 'ope-learn-domain-ana-smirstpv'     # Summer 2024 FCDS, "Pittsburgh" students, FcdsP3Agent
NAMESPACE = 'default'
ROOM_PREFIX = "room"
TIMEOUT_RESPONSE_CODE = 503

# GLOBAL VARIABLES
assigner_initialized = False
nextRoomNum = 28000
nextThreadNum = 0
nextCheckForOldUsers = time.time() + CHECK_FOR_USER_DELETION_WAIT_TIME
nextCheckForOldRooms = time.time() + CHECK_FOR_ROOM_DELETION_WAIT_TIME
unassigned_users = []       # Users with no room assignment
users_to_notify = []        # Users either with an assigned URL or who have timed out

# Threading variables
user_queue = queue.Queue()
thread_lock = threading.Lock()
condition = threading.Condition()
threadMapping = {}
eventMapping = {}

basedir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] =\
        'postgresql://postgres:testpwd@lobby_db:5432/lobby_db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_EXPIRE_ON_COMMIT'] = False
lobby_db = SQLAlchemy(app)
session = None


cors = CORS(app)
app.config['CORS_ORIGINS'] = [
    'https://learn.sailplatform.org',
    'https://staging.learn.sailplatform.org',
    'https://qa.learn.sailplatform.org',
    'https://projects.sailplatform.org',
    'https://staging.projects.sailplatform.org',
    'https://qa.projects.sailplatform.org',
    'https://collab.lti.cs.cmu.edu',
]


class Room(lobby_db.Model):
    __tablename__ = 'room'
    id = lobby_db.Column(lobby_db.Integer, primary_key=True)
    room_name = lobby_db.Column(lobby_db.VARCHAR(80))
    activity_url = lobby_db.Column(lobby_db.String(200))
    module_slug = MODULE_SLUG
    bot_namespace = NAMESPACE
    bot_name = OPE_BOT_NAME
    start_time = lobby_db.Column(lobby_db.DateTime(timezone=False), server_default=func.now())
    start_time_string = lobby_db.Column(lobby_db.String(30))
    num_users = lobby_db.Column(lobby_db.Integer)
    users = lobby_db.relationship('User', back_populates='room')

    def __repr__(self):
        return f'<Room {self.room_name}>'


class User(lobby_db.Model):
    __tablename__ = 'user'
    id = lobby_db.Column(lobby_db.Integer, primary_key=True)
    user_id = lobby_db.Column(lobby_db.VARCHAR(80), nullable=False)
    name = lobby_db.Column(lobby_db.VARCHAR(80), primary_key=False)
    email = lobby_db.Column(lobby_db.VARCHAR(80), primary_key=False)
    password = lobby_db.Column(lobby_db.String(100), primary_key=False)
    entity_id = lobby_db.Column(lobby_db.VARCHAR(80), primary_key=False)
    module_slug = lobby_db.Column(lobby_db.String(50), primary_key=False)
    start_time = lobby_db.Column(lobby_db.DateTime(timezone=False), server_default=func.now())
    room_name = lobby_db.Column(lobby_db.VARCHAR(80))
    room_id = lobby_db.Column(lobby_db.Integer, lobby_db.ForeignKey('room.id'), nullable=True)
    activity_url = lobby_db.Column(lobby_db.String(200), primary_key=False)
    activity_url_notified = lobby_db.Column(lobby_db.Boolean, primary_key=False)
    ope_namespace = NAMESPACE
    agent = lobby_db.Column(lobby_db.VARCHAR(80), primary_key=False)
    thread_name = lobby_db.Column(lobby_db.VARCHAR(80), primary_key=False)
    event_name = lobby_db.Column(lobby_db.VARCHAR(80), primary_key=False)
    room = lobby_db.relationship('Room', back_populates='users')

    def __repr__(self):
        return f'<User {self.user_id}>'


@app.route('/getJupyterlabUrl', methods=['POST'])
def getJupyterlabUrl():
    global user_queue, session, nextThreadNum, threadMapping, eventMapping, NAMESPACE
    print("getJupyterlabUrl: enter", flush=True)
    nextThreadNum += 1
    event_name = "event" + str(nextThreadNum)
    thread_name = "thread" + str(nextThreadNum)
    event = threading.Event()
    eventMapping[event_name] = event
    with thread_lock:
        current_user = threading.Thread()
        threadMapping[thread_name] = current_user
        current_user.event = event
        data = json.loads(request.data.decode('utf-8'))
        print(f"getJupyterlabUrl -- data as string: {str(data)}", flush=True)
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
                print("getJupyterlabUrl: user " + str(user_id) + " is NOT a new user", flush=True)
                user_info = {'user_id': str(user_id), 'name': str(name), 'email': str(email),
                             'password': str(password), 'entity_id': str(entity_id)}
                duplicate_user = is_duplicate_user(user_info, user)

                # If old non-duplicate user, delete and start over
                if not duplicate_user:
                    print("getJupyterlabUrl: user " + str(user_id) + " is an old non-duplicate user -- starting over",
                          flush=True)
                    session.delete(user)
                    session.commit()
                    session = lobby_db.session
                    if user in unassigned_users:
                        unassigned_users.remove(user)
                    user = User(user_id=user_id, name=name, email=email, password=password,
                                entity_id=entity_id, ope_namespace=NAMESPACE, module_slug=MODULE_SLUG,
                                activity_url_notified=False, thread_name=thread_name, event_name=event_name)
                    session.add(user)
                    session.commit()
                    session = lobby_db.session
                    print("getJupyterlabUrl - user.name: " + user.name +
                          "  --  user.start_time: " + str(datetime.fromtimestamp(user.start_time.timestamp())))

                # If duplicate user, they'll need a URL notification and maybe a room assignment
                if duplicate_user:
                    print("getJupyterlabUrl: user " + str(user_id) + " is a duplicate user", flush=True)
                    user.activity_url_notified = False
                    user.thread_name = thread_name
                    user.event_name = event_name
                    session.add(user)
                    session.commit()
                    session = lobby_db.session

            # New user
            if user is None:
                print("getJupyterlabUrl: user " + str(user_id) + " is a new user", flush=True)
                user = User(user_id=user_id, name=name, email=email, password=password,
                            entity_id=entity_id, ope_namespace=NAMESPACE, module_slug=MODULE_SLUG,
                            activity_url_notified=False, thread_name=thread_name, event_name=event_name)
                session.add(user)
                session.commit()
                session = lobby_db.session
                print("getJupyterlabUrl - user.name: " + user.name + "  --  user.start_time: "
                      + str(datetime.fromtimestamp(user.start_time.timestamp())))

        user_queue.put((current_user, user_id))
        # print("getJupyterlabUrl - user_queue length: " + str(user_queue.qsize()), flush=True)

    # print("getJupyterlabUrl: about to 'event.wait()'", flush=True)
    event.wait()
    # print("getJupyterlabUrl: returned from 'event.wait()'", flush=True)

    if current_user.code == 200:
        print("getJupyterlabUrl: code 200; returning URL: " + current_user.url, flush=True)
        return current_user.url
    else:
        print("getJupyterlabUrl: returning negative code: " + str(current_user.code), flush=True)
        response = make_response('', current_user.code)
        return response


@app.route('/targetUsers/<target_users>', methods=['PUT'])
def targetUsers(target_users):
    global TARGET_USERS_PER_ROOM
    TARGET_USERS_PER_ROOM = int(target_users)
    print("targetUsers: target_users = " + target_users, flush=True)
    return "OK", 200


@app.route('/minUsers/<min_users>', methods=['PUT'])
def minUsers(min_users):
    global MIN_USERS_PER_ROOM
    MIN_USERS_PER_ROOM = int(min_users)
    print("minUsers: min_users = " + min_users, flush=True)
    return "OK", 200


@app.route('/maxUsers/<max_users>', methods=['PUT'])
def maxUsers(max_users):
    global MAX_USERS_PER_ROOM
    MAX_USERS_PER_ROOM = int(max_users)
    print("maxUsers: max_users = " + max_users, flush=True)
    return "OK", 200


@app.route('/subAssignWait/<max_sub_assign>', methods=['PUT'])
def subAssignWait(max_sub_assign):
    global MAX_WAIT_TIME_FOR_SUBOPTIMAL_ASSIGNMENT
    MAX_WAIT_TIME_FOR_SUBOPTIMAL_ASSIGNMENT = int(max_sub_assign)
    print("subAssignWait: max_sub_assign = " + max_sub_assign, flush=True)
    return "OK", 200


@app.route('/roomNum/<room_num>', methods=['PUT'])
def roomNum(room_num):
    global nextRoomNum
    nextRoomNum = int(room_num)
    print("roomNum: room_num = " + room_num, flush=True)
    return "OK", 200


@app.route('/giveUpWait/<max_give_up>', methods=['PUT'])
def giveUpWait(max_give_up):
    global MAX_WAIT_TIME_UNTIL_GIVE_UP
    MAX_WAIT_TIME_UNTIL_GIVE_UP = int(max_give_up)
    print("giveUpWait: max_give_up = " + max_give_up, flush=True)
    return "OK", 200


@app.route('/maxRoomAge/<max_age>', methods=['PUT'])
def maxRoomAge(max_age):
    global MAX_ROOM_AGE_FOR_NEW_USERS
    MAX_ROOM_AGE_FOR_NEW_USERS = int(max_age)
    print("maxRoomAge: max_age = " + max_age, flush=True)
    return "OK", 200


@app.route('/requestPrefix/<request_prefix>', methods=['PUT'])
def requestPrefix(request_prefix):
    global REQUEST_PREFIX
    REQUEST_PREFIX = unquote(request_prefix)
    print("requestPrefix: request_prefix = " + request_prefix, flush=True)
    return "OK", 200


@app.route('/namespace/<namespace>', methods=['PUT'])
def namespace(namespace):
    global NAMESPACE
    NAMESPACE = namespace
    print("namespace: namespace = " + namespace, flush=True)
    return "OK", 200


@app.route('/deleteRoom/<room_name>', methods=['PUT'])
def deleteRoom(room_name):
    print("deleteRoom: room_name = " + room_name, flush=True)
    delete_result = delete_room(room_name)
    if delete_result is not None:
        return "OK", 200
    else:
        return "Room " + room_name + " not found", 404


@app.route('/deleteUser/<user_id>', methods=['PUT'])
def deleteUser(user_id):
    print("deleteUser: user_name = " + user_id, flush=True)
    delete_result = delete_user(user_id)
    if delete_result is not None:
        return "OK", 200
    else:
        return "User " + user_id + " not found", 404


@app.route('/moduleSlug/<module_slug>', methods=['PUT'])
def moduleSlug(module_slug):
    global MODULE_SLUG
    MODULE_SLUG = module_slug
    print("moduleSlug: module_slug = " + module_slug, flush=True)
    return "OK", 200


@app.route('/printRooms', methods=['PUT'])
def printRooms():
    print("printRooms:", flush=True)
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        print_room_assignments()
    responseValue = "\n" + buffer.getvalue() + "\n"
    print(responseValue, flush=True)
    return Response(responseValue, status=200, mimetype='text/plain')

 
@app.route('/printValues', methods=['PUT'])
def printValues():
    global MODULE_SLUG, REQUEST_PREFIX, MAX_WAIT_TIME_FOR_SUBOPTIMAL_ASSIGNMENT, \
        MAX_WAIT_TIME_UNTIL_GIVE_UP, MAX_ROOM_AGE_FOR_NEW_USERS, MAX_USERS_PER_ROOM, \
        MIN_USERS_PER_ROOM, TARGET_USERS_PER_ROOM, nextRoomNum, NAMESPACE
    responseValue = (
        f"\nTarget Users - targetUsers:             {str(TARGET_USERS_PER_ROOM)}\n"
        f"Min Users - minUsers:                   {str(MIN_USERS_PER_ROOM)}\n"
        f"Max Users - maxUsers:                   {str(MAX_USERS_PER_ROOM)}\n" 
        f"Suboptimal assign wait - subAssignWait: {str(MAX_WAIT_TIME_FOR_SUBOPTIMAL_ASSIGNMENT)}\n"
        f"Next room number - roomNum:             {str(nextRoomNum)}\n"
        f"Give-up wait - giveUpWait:              {str(MAX_WAIT_TIME_UNTIL_GIVE_UP)}\n"
        f"Max room assign age - maxRoomAge:       {str(MAX_ROOM_AGE_FOR_NEW_USERS)}\n"
        f"Request Prefix - requestPrefix:         {REQUEST_PREFIX}\n"
        f"Namespace - namespace:                  {NAMESPACE}\n"
        f"Delete Room - deleteRoom:               CAUTION\n" 
        f"Delete Room - deleteUser:               CAUTION\n" 
        f"Module Slug - moduleSlug:               {MODULE_SLUG}\n"
        f"Print Rooms - printRooms\n"
        f"Print Parameters - printValues\n\n"
    )
    print(responseValue, flush=True)
    return Response(responseValue, status=200, mimetype="text/plain")


def request_session_update_users(room):
    global REQUEST_PREFIX, SESSION_ONLY_REQUEST_PATH, MODULE_SLUG, NAMESPACE, OPE_BOT_USERNAME, LOCAL_TIME_ZONE
    # print("request_session_update_users -- request_url: " + request_url, flush=True)
    with app.app_context():
        request_url = REQUEST_PREFIX + "/" + SESSION_ONLY_REQUEST_PATH + "/" + NAMESPACE + "/" + MODULE_SLUG + \
                      "-" + room.room_name
        print("request_session_update_users -- request_url: " + request_url, flush=True)
        user_list = []
        i = 0
        while i < room.num_users:
            user = room.users[i]
            user_element = {'namespace': NAMESPACE, 'name': email_to_dns(user.email)}
            user_list.append(user_element)
            i += 1
        data = {
            "spec": {
                "startTime": datetime.now(LOCAL_TIME_ZONE).replace(microsecond=0).isoformat(),
                "moduleSlug": MODULE_SLUG,
                "sessionName": MODULE_SLUG + "-" + room.room_name,
                "opeBotRef": {
                    "namespace": NAMESPACE,
                    "name": OPE_BOT_NAME
                },
                "opeUsersRef": user_list
            }
        }
    print("request_session_update_users -- data as string: " + str(data), flush=True)
    headers = {'Content-Type': 'application/json'}
    response = None
    try:
        response = requests.put(request_url, data=json.dumps(data), headers=headers)
        response.raise_for_status()
        print("request_session_update_users: POST successful", flush=True)
    except RequestException as e:
        # Catches any exception that the requests library might raise (e.g., ConnectionError, Timeout, HTTPError)
        print(f"request_session_update_users: An error occurred during the request: {e}")
    except Exception as e:
        print(f"request_session_update_users: An unexpected error occurred: {e}")
    if response:
        print(f"request_session_update_users: response code: {response.status_code}")


# TODO: Check if assignment request chain fails and react accordingly
def request_session_plus_users(room):
    global REQUEST_PREFIX, SESSION_PLUS_USERS_REQUEST_PATH, MODULE_SLUG, NAMESPACE, OPE_BOT_USERNAME, \
        LOCAL_TIME_ZONE, session
    request_url = REQUEST_PREFIX + "/" + SESSION_PLUS_USERS_REQUEST_PATH
    print("request_session_plus_users -- request_url: " + request_url, flush=True)
    start_time = datetime.now(LOCAL_TIME_ZONE).replace(microsecond=0).isoformat()
    # room.start_time_string = start_time
    with app.app_context():
        user_list = []
        # room.start_time_string = start_time
        # session.add(room)
        # session.commit()
        session = lobby_db.session
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
                    "moduleSlug": MODULE_SLUG,
                    "sessionName": room.room_name,
                    # "startTime": datetime.now(LOCAL_TIME_ZONE).replace(microsecond=0).isoformat(),
                    "startTime": start_time,
                }
            ]
        }
    print("request_session_plus_users -- data as string: " + str(data), flush=True)
    headers = {'Content-Type': 'application/json'}

    response = None
    try:
        response = requests.post(request_url, data=json.dumps(data), headers=headers)
        response.raise_for_status()
        print("request_session_plus_users: POST successful", flush=True)
    except RequestException as e:
        # Catches any exception that the requests library might raise (e.g., ConnectionError, Timeout, HTTPError)
        print(f"request_session_plus_users: An error occurred during the request: {e}")
    except Exception as e:
        print(f"request_session_plus_users: An unexpected error occurred: {e}")
    if response:
        print(f"request_session_plus_users: response Code: {response.status_code}")


def request_user(user, room):
    global USER_REQUEST_PATH, NAMESPACE, MODULE_SLUG, REQUEST_PREFIX
    with app.app_context():
        request_url = REQUEST_PREFIX + "/" + USER_REQUEST_PATH + "/" + NAMESPACE + "/" + \
                      email_to_dns(user.email)
        print("request_user -- request_url: " + request_url, flush=True)
        data = {
            'spec': {
                'enableMatch': False,
                'isBot': False,
                'stressTest': False,
                'name': user.name,
                'email': user.email,
                'password': user.password,
                'moduleSlug': MODULE_SLUG,
                'opeSessionRef': {
                    'namespace': NAMESPACE,
                    'name': MODULE_SLUG + "-" + room.room_name
                }
            }
        }
    print("request_user -- data as string: " + str(data), flush=True)
    headers = {'Content-Type': 'application/json'}
    response = None
    try:
        response = requests.post(request_url, data=json.dumps(data), headers=headers)
        response.raise_for_status()
        print("request_user: POST successful", flush=True)
    except RequestException as e:
        # Catches any exception that the requests library might raise (e.g., ConnectionError, Timeout, HTTPError)
        print(f"An error occurred during the request: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    if response:
        print(f"Status Code: {response.status_code}")


def request_room_status(room):
    global REQUEST_PREFIX, SESSION_READINESS_PATH, MODULE_SLUG, NAMESPACE
    with app.app_context():
        request_url = REQUEST_PREFIX + "/" + SESSION_READINESS_PATH + "/" + NAMESPACE + "/" + MODULE_SLUG + \
                      "-" + room.room_name
        print("request_room_status -- request_url: " + request_url, flush=True)
    response = None
    try:
        response = requests.get(request_url)
        response.raise_for_status()
    except RequestException as e:
        # Catches any exception that the requests library might raise (e.g., ConnectionError, Timeout, HTTPError)
        print(f"request_room_status: An error occurred during the request: {e}")
    except Exception as e:
        print(f"request_room_status: An unexpected error occurred: {e}")
    if response:
        print(f"request_room_status: response Code: {response.status_code}")

    if response.status_code == 200:
        print("request_room_status: response code " + str(response.status_code), flush=True)
        response_data = response.text
        print("request_room_status -- URL: " + str(response_data), flush=True)
        url_response_code = check_url(response_data)
        if url_response_code < 300:
            print("request_room_status check_url response code ok: " + str(url_response_code), flush=True)
            return response_data
        else:
            print("request_room_status check_url response code NOT ok: " + str(url_response_code), flush=True)
            return None
    else:
        print("request_room_status failed -- response code " + str(response.status_code), flush=True)
        return None




def check_url(response_data):
    print(f"check_url - incoming response_data: {response_data}", flush=True)
    response = None
    try:
        response = requests.get(response_data)
        response.raise_for_status()
        print("check_url: POST successful", flush=True)
    except RequestException as e:
        # Catches any exception that the requests library might raise (e.g., ConnectionError, Timeout, HTTPError)
        print(f"check_url:  An error occurred during the request: {e}")
    except Exception as e:
        print(f"check_url:  An unexpected error occurred: {e}")
    if response:
        print(f"check_url:  Status Code: {response.status_code}")
    return response.status_code


def check_for_new_activity_urls():
    global session
    with app.app_context():
        rooms = Room.query.all()
        for room in rooms:
            if room.room_name != "waiting_room" and room.activity_url is None:
                wait_time = time.time() - room.start_time.timestamp()
                print("check_for_new_activity_urls -- room " + room.room_name + " wait time: " + str(wait_time),
                      flush=True)
                if wait_time >= MAX_WAIT_TIME_UNTIL_GIVE_UP:
                    prune_room(room)
                else:
                    activity_url = request_room_status(room)
                    if activity_url is not None:
                        print("check_for_new_activity_urls - activity_url for room " + room.room_name +
                              " is " + str(activity_url), flush=True)
                        room.activity_url = activity_url
                        session.add(room)
                        assign_users_activity_url(room)
                    else:
                        print("check_for_new_activity_urls - activity_url for room " + room.room_name +
                              " is None", flush=True)
        session.commit()
        session = lobby_db.session


def assign_users_activity_url(room):
    global session, users_to_notify, threadMapping
    activity_url = room.activity_url
    if activity_url is not None:
        users = room.users
    else:
        print("assign_users_activity_url: activity_url is None -- returning", flush=True)
        return
    for user in users:
        user.activity_url = activity_url
        print("assign_users_activity_url: user: " + user.name + "  --   URL: " + user.activity_url, flush=True)
        session.add(user)
        user_thread = threadMapping[user.thread_name]
        user_thread.code = 200
        user_thread.url = activity_url
        user_event = eventMapping[user.event_name]
        users_to_notify.append(user_event)


def email_to_dns(email):
    email1 = email.replace('@', '-at-')
    email2 = email1.replace('.', '-')
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
        return True

# TODO: Check if assignment request chain fails and react accordingly
def assign_rooms():
    global unassigned_users, TARGET_USERS_PER_ROOM, MAX_USERS_PER_ROOM
    num_unassigned_users = len(unassigned_users)
    if num_unassigned_users > 0:

        # Fill rooms that have less than the target number of users
        if FILL_ROOMS_UNDER_TARGET:
            assign_rooms_under_n_users(TARGET_USERS_PER_ROOM)

        # Fill rooms with the target number of users
        if num_unassigned_users >= TARGET_USERS_PER_ROOM:
            assign_new_rooms(TARGET_USERS_PER_ROOM)

        # Check for any users waiting long enough that they should get a suboptimal assignment
        users_due_for_suboptimal = get_users_due_for_suboptimal()
        num_users_due_for_suboptimal = len(users_due_for_suboptimal)
        # print("assign_rooms - num_users_due_for_suboptimal assignment: " + str(num_users_due_for_suboptimal),
        #       flush=True)

        # Overfill rooms if there are not enough unassigned users to create a new room
        if (num_unassigned_users > 0) and OVERFILL_ROOMS:
            if (num_users_due_for_suboptimal > 0) and (len(unassigned_users) < MIN_USERS_PER_ROOM):
                # print("assign_rooms - calling assign_rooms_under_n_users", flush=True)
                assign_rooms_under_n_users(MAX_USERS_PER_ROOM)

        # Create a new room, even with less than the target number of users, if there are enough unassigned users
        if num_unassigned_users > 0:
            if (num_users_due_for_suboptimal > 0) and (num_unassigned_users >= MIN_USERS_PER_ROOM):
                assign_new_room(num_unassigned_users)
        prune_users_waiting_too_long()       # tell users who have been waiting too long to come back later


def get_users_due_for_suboptimal():
    global unassigned_users
    users_due_for_suboptimal = []
    i = 0
    if len(unassigned_users) > TARGET_USERS_PER_ROOM:   # Don't assign suboptimally if there are now target num of users
        # print("get_users_due_for_suboptimal(): now there are enough users for the target", flush=True)
        return users_due_for_suboptimal
    with app.app_context():
        while i < len(unassigned_users):
            user = unassigned_users[i]
            time_diff = time.time() - user.start_time.timestamp()
            print("Time diff for unassignedUser(" + user.user_id + "): " + str(time_diff), flush=True)
            if time_diff > MAX_WAIT_TIME_FOR_SUBOPTIMAL_ASSIGNMENT:
                users_due_for_suboptimal.append(unassigned_users[i])
                # print("user " + user.user_id + "is due for suboptimal assignment", flush=True)
            i += 1
    return users_due_for_suboptimal

# TODO: Check if assignment request chain fails and react accordingly
def assign_rooms_under_n_users(n_users):
    # Assuming
    #   -- assign to rooms that are most-under n-users first
    #   -- fill one room to n-users before assigning to next room
    global unassigned_users
    # print("assign_rooms_under_n_users - n_users: " + str(n_users), flush=True)
    available_rooms_under_n_users = []
    if len(unassigned_users) > 0:
        available_rooms_under_n_users = get_sorted_available_rooms(n_users)
        # print("assign_rooms_under_n_users - len(available_rooms_under_n_users): " +
        #       str(len(available_rooms_under_n_users)), flush=True)
        # print("assign_rooms_under_n_users - len(unassigned_users): " +
        #       str(len(unassigned_users)), flush=True)
    i = 0
    is_room_new = False
    with app.app_context():
        while (i < len(available_rooms_under_n_users)) and (len(unassigned_users) > 0):
            # print("assign_rooms_under_n_users - calling assign_up_to_n_users", flush=True)
            assign_up_to_n_users(available_rooms_under_n_users[i], n_users, is_room_new)
            i += 1

# TODO: Check if assignment request chain fails and react accordingly
def assign_up_to_n_users(room, num_users, is_room_new):
    global unassigned_users
    while (len(room.users) < num_users) and (len(unassigned_users) > 0):
        user = unassigned_users[0]
        # print("assign_up_to_n_users - calling assign_room", flush=True)
        assign_room(user, room, is_room_new)
        unassigned_users.remove(user)


# 1. Sort primarily by number of users (ascending), then secondarily by start_time (ascending)
#    to prioritize rooms primarily by the least users and secondarily by the oldest start time.
# 2. Select rooms that are not tool old to add new users.
# 3. Strip waiting_room from results.
# 4. Strip rooms with >= max_users from the results.
def get_sorted_available_rooms(max_users):
    global MAX_ROOM_AGE_FOR_NEW_USERS
    room_list = []
    # print("get_sorted_available_rooms - max_users: " + str(max_users), flush=True)
    with app.app_context():
        sorted_rooms = Room.query.order_by(Room.num_users.asc(), Room.start_time.asc()).all()
        for room in sorted_rooms:
            if room.room_name != "waiting_room":
                print("   " + room.room_name + "  -  users: " + (str(len(room.users))), flush=True)
        current_time = time.time()
        for room in sorted_rooms:
            if room.room_name != "waiting_room":
                time_diff = current_time - room.start_time.timestamp()
                # print("get_sorted_available_rooms -- room: " + room.room_name + "  --  time_diff: " +
                #       str(time_diff), flush=True)
                if time_diff < MAX_ROOM_AGE_FOR_NEW_USERS:
                    # print("get_sorted_available_rooms - time-diff < max room age ", flush=True)
                    if len(room.users) < max_users:
                        # print("get_sorted_available_rooms -- adding room: " + room.room_name, flush=True)
                        room_list.append(room)
        # print("get_sorted_available_rooms:", flush=True)
        # if len(room_list) > 0:
            # for room in room_list:
            #     print("   " + room.room_name + "  -  users: " + (str(len(room.users))), flush=True)
        # else:
            # print("   no rooms available", flush=True)
    return room_list


# TODO: Check if assignment request chain fails and react accordingly
def assign_new_rooms(num_users_per_room):
    global unassigned_users
    while len(unassigned_users) >= num_users_per_room:
        # print("assign_new_rooms, calling assign_new_room -- len(unassigned_users): " + str(len(unassigned_users)) +
        #       " -- num_users_per_room: " + str(num_users_per_room), flush=True)
        assign_new_room(num_users_per_room)

# TODO: Check if assignment request chain fails and react accordingly
def assign_new_room(num_users):
    global nextRoomNum, session

    room_name = ROOM_PREFIX + str(nextRoomNum)
    is_room_new = True
    nextRoomNum += 1

    # print("assign_new_room -- str(num_users): " + str(num_users) + " -- room_name: " + room_name, flush=True)

    with app.app_context():
        room = Room(room_name=room_name, activity_url=None, num_users=0)
        session.add(room)
        session.commit()
        session = lobby_db.session
        assign_up_to_n_users(room, num_users, is_room_new)
        request_session_plus_users(room)

    # print("assign_new_room - room.num_users: " + str(room.num_users), flush=True)

# TODO: Check if assignment request chain fails and react accordingly
def assign_room(user, room, is_room_new):
    global unassigned_users, session, users_to_notify
    user.room_name = room.room_name
    room.users.append(user)
    room.num_users += 1
    if not is_room_new:
        request_user(user, room)
        if room.activity_url is not None:
            user.activity_url = room.activity_url
            user_thread = threadMapping[user.thread_name]
            user_thread.code = 200
            user_thread.url = user.activity_url
            user_event = eventMapping[user.event_name]
            users_to_notify.append(user_event)
        request_session_update_users(room)
    session.add(user)
    session.add(room)
    session.commit()
    session = lobby_db.session


def prune_users_waiting_too_long():
    global unassigned_users, session, threadMapping, users_to_notify
    for user in unassigned_users:
        with app.app_context():
            if (time.time() - user.start_time.timestamp()) >= MAX_WAIT_TIME_UNTIL_GIVE_UP:
                print("prune_users_waiting_too_long: user_id: " + user.user_id, flush=True)
                unassigned_users.remove(user)
                User.query.filter(User.id == user.id).delete()
                session.commit()
                session = lobby_db.session
                user_thread = threadMapping[user.thread_name]
                user_thread.code = TIMEOUT_RESPONSE_CODE
                user_event = eventMapping[user.event_name]
                users_to_notify.append(user_event)
    return unassigned_users


def prune_old_users():
    global session, nextCheckForOldUsers, ELAPSED_TIME_UNTIL_USER_DELETION, CHECK_FOR_USER_DELETION_WAIT_TIME
    if time.time() >= nextCheckForOldUsers:
        with app.app_context():
            users = User.query.all()
            for user in users:
                user_elapsed_time = time.time() - user.start_time.timestamp()
                if user_elapsed_time >= ELAPSED_TIME_UNTIL_USER_DELETION:
                    print("prune_old_users: deleting user_id: " + user.user_id + " after " + str(user_elapsed_time) +
                          " seconds", flush=True),
                    User.query.filter(User.id == user.id).delete()
            session.commit()
            session = lobby_db.session
        nextCheckForOldUsers += CHECK_FOR_USER_DELETION_WAIT_TIME


def prune_old_rooms():
    global session, nextCheckForOldRooms, ELAPSED_TIME_UNTIL_ROOM_DELETION, CHECK_FOR_ROOM_DELETION_WAIT_TIME
    if time.time() >= nextCheckForOldRooms:
        with app.app_context():
            rooms = Room.query.all()
            for room in rooms:
                room_elapsed_time = time.time() - room.start_time.timestamp()
                if (room.room_name != "waiting_room") and (room_elapsed_time >= ELAPSED_TIME_UNTIL_ROOM_DELETION):
                    room_users = room.users
                    if len(room_users) == 0:
                        print("prune_old_rooms: deleting room: " + room.room_name + " after " + str(room_elapsed_time) +
                              " seconds", flush=True),
                        Room.query.filter(Room.id == room.id).delete()
            session.commit()
            session = lobby_db.session
        nextCheckForOldRooms += CHECK_FOR_ROOM_DELETION_WAIT_TIME


def prune_room(room):
    global session, users_to_notify
    with app.app_context():
        for user in room.users:
            print("prune_room -- deleting user " + user.name, flush=True)
            user_thread = threadMapping[user.thread_name]
            user_thread.code = TIMEOUT_RESPONSE_CODE
            user_event = eventMapping[user.event_name]
            users_to_notify.append(user_event)
            User.query.filter(User.id == user.id).delete()
            session.commit()
            session = lobby_db.session
        Room.query.filter(Room.id == room.id).delete()
        session.commit()
        session = lobby_db.session


# This is used by the endpoint /lobbyDeleteRoom. It differs from 'prune_room() in two respects:
# -- No user needs to be notified.
# -- The input parameter is a room name rather than a room entry in the database.
def delete_room(room_name):
    global session, users_to_notify
    with app.app_context():
        room = Room.query.filter_by(room_name=room_name).first()
        if room is not None:
            for user in room.users:
                print("delete_room -- deleting user " + user.name, flush=True)
                User.query.filter(User.id == user.id).delete()
                session.commit()
                session = lobby_db.session
            Room.query.filter(Room.id == room.id).delete()
            session.commit()
            session = lobby_db.session
            return room_name
        else:
            return None


def delete_user(user_id):
    global session
    with app.app_context():
        user = User.query.filter_by(user_id=user_id).first()
        if user is not None:
            User.query.filter(User.id == user.id).delete()
            session.commit()
            session = lobby_db.session
            return user_id
        else:
            return None


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


def initialize_lobby():
    global session
    with app.app_context():
        session = lobby_db.session
        lobby_db.drop_all()
        lobby_db.create_all()
        waiting_room = Room(room_name="waiting_room", activity_url=None, num_users=0)
        print("initialize_lobby - Created waiting_room - room_name: " + waiting_room.room_name, flush=True)
        session.add(waiting_room)
        session.commit()
        # session = None
        session = lobby_db.session


def assigner():
    global session, unassigned_users, users_to_notify, eventMapping, user_queue, assigner_initialized

    # Initialize Lobby
    if not assigner_initialized:
        with app.app_context():
            session = lobby_db.session
            lobby_db.drop_all()
            lobby_db.create_all()
            waiting_room = Room(room_name="waiting_room", activity_url=None, num_users=0)
            session.add(waiting_room)
            session.commit()
            session = lobby_db.session
            assigner_initialized = True

    # Repeat continuously while Lobby is running
    while True:
        users_to_notify = []        # Clear any previously notified users from list to notify
        with condition:

            # Get users in the queue
            while not user_queue.empty():
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
                        print("assigner: adding user " + str(user_id) + " to unassigned_users", flush=True)
                        unassigned_users.append(user)

                    # If user has previously logged in and received an activity URL, just send it
                    if user.activity_url is not None:
                        print("assigner: resending activity_url to user " + str(user_id) +
                              " -- activity_url: " + user.activity_url, flush=True)
                        user_thread = threadMapping[user.thread_name]
                        user_thread.code = 200
                        user_thread.url = user.activity_url
                        user_event = eventMapping[user.event_name]
                        users_to_notify.append(user_event)

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

            # Remove old users and rooms from the DB
            prune_old_users()
            prune_old_rooms()

            time.sleep(ASSIGNER_SLEEP_TIME)


consumer_thread = threading.Thread(target=assigner, daemon=True)
consumer_thread.start()

if __name__ == '__main__':
    session = lobby_db.session
    app.run(debug=True)
