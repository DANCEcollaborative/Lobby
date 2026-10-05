import ast
from contextlib import nullcontext
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch
import runtime_settings

SOURCE = Path(__file__).resolve().parents[1] / 'lobby.py'
FUNCTIONS = {'assign_rooms', 'assign_new_rooms', 'assign_rooms_under_n_users', 'assign_up_to_n_users', 'get_users_due_for_suboptimal', 'get_sorted_available_rooms', 'is_duplicate_user'}

class GroupingTests(unittest.TestCase):
    def harness(self):
        clock, rooms = [1000.], []
        class Column:
            def asc(self): return self
        class Query:
            def order_by(self, *args): return self
            def all(self): return sorted(rooms, key=lambda r: (len(r.users), r.start_time.timestamp()))
        env = dict(time=types.SimpleNamespace(time=lambda:clock[0]), app=types.SimpleNamespace(app_context=nullcontext), Room=types.SimpleNamespace(query=Query(),num_users=Column(),start_time=Column()), TARGET_USERS_PER_ROOM=3,MIN_USERS_PER_ROOM=1,MAX_USERS_PER_ROOM=3,MAX_WAIT_TIME_FOR_SUBOPTIMAL_ASSIGNMENT=60,MAX_ROOM_AGE_FOR_NEW_USERS=0,FILL_ROOMS_UNDER_TARGET=True,OVERFILL_ROOMS=True,unassigned_users=[],prune_users_waiting_too_long=lambda:None)
        tree=ast.parse(SOURCE.read_text());selected=ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in FUNCTIONS],type_ignores=[])
        exec(compile(selected,str(SOURCE),'exec'),env)
        def assign(user,room,new):room.users.append(user)
        def new_room(count, selected_users=None):
            room=types.SimpleNamespace(room_name='r'+str(len(rooms)),users=[],start_time=types.SimpleNamespace(timestamp=lambda t=clock[0]:t))
            rooms.append(room)
            if selected_users is None:env['assign_up_to_n_users'](room,count,True)
            else:room.users.extend(selected_users)
        env.update(assign_room=assign,assign_new_room=new_room)
        def arrive(name, mode="group"):
            env['unassigned_users'].append(types.SimpleNamespace(user_id=name,participation_mode=mode,start_time=types.SimpleNamespace(timestamp=lambda t=clock[0]:t)))
        return env,clock,rooms,arrive

    def test_solo_skips_wait_and_is_never_matched_with_waiting_group(self):
        env,clock,rooms,arrive=self.harness()
        arrive('group');arrive('solo','solo');env['assign_rooms']()
        self.assertEqual([[u.user_id for u in r.users] for r in rooms],[['solo']])
        self.assertEqual([u.user_id for u in env['unassigned_users']],['group'])
        clock[0]+=61;env['assign_rooms']()
        self.assertEqual([[u.user_id for u in r.users] for r in rooms],[['solo'],['group']])

    def test_two_solo_requests_remain_separate_from_a_full_group(self):
        env,clock,rooms,arrive=self.harness()
        for name,mode in [('A','group'),('S','solo'),('B','group'),('T','solo'),('C','group')]:arrive(name,mode)
        env['assign_rooms']()
        self.assertEqual([[u.user_id for u in r.users] for r in rooms],[['S'],['T'],['A','B','C']])
        self.assertEqual(env['unassigned_users'],[])
        env['MAX_ROOM_AGE_FOR_NEW_USERS']=600
        arrive('late');env['assign_rooms']()
        self.assertEqual([len(r.users) for r in rooms],[1,1,3])

    def test_four_arrivals_form_three_then_solo_after_wait(self):
        env,clock,rooms,arrive=self.harness()
        for name in 'ABCD':arrive(name)
        env['assign_rooms']();self.assertEqual([len(r.users) for r in rooms],[3])
        clock[0]+=59;env['assign_rooms']();self.assertEqual(len(rooms),1)
        clock[0]+=2;env['assign_rooms']();self.assertEqual([len(r.users) for r in rooms],[3,1])

    def test_staggered_pair_then_late_student_gets_new_room(self):
        env,clock,rooms,arrive=self.harness()
        arrive('A');clock[0]+=30;arrive('B');env['assign_rooms']();self.assertEqual(rooms,[])
        clock[0]+=31;env['assign_rooms']();self.assertEqual([len(r.users) for r in rooms],[2])
        arrive('C');env['assign_rooms']();self.assertEqual([len(r.users) for r in rooms],[2])
        clock[0]+=61;env['assign_rooms']();self.assertEqual([len(r.users) for r in rooms],[2,1])

    def test_same_identity_is_recognized_for_reconnection(self):
        env,_,_,_=self.harness();u=types.SimpleNamespace(name='A',email='a@example.invalid',password='test')
        self.assertTrue(env['is_duplicate_user'](dict(name=u.name,email=u.email,password=u.password,entity_id='dev'),u))

    def test_environment_policy_and_legacy_defaults(self):
        with patch.dict('os.environ',{},clear=True):self.assertEqual(runtime_settings.setting('MAX_USERS_PER_ROOM',4),4)
        with patch.dict('os.environ',{'LOBBY_MAX_USERS_PER_ROOM':'3','LOBBY_MAX_ROOM_AGE_FOR_NEW_USERS':'0'}):
            self.assertEqual(runtime_settings.setting('MAX_USERS_PER_ROOM',4),3)
            self.assertEqual(runtime_settings.setting('MAX_ROOM_AGE_FOR_NEW_USERS',600),0)

    def test_counter_survives_restart(self):
        with tempfile.TemporaryDirectory() as d:
            with patch.dict('os.environ',{'LOBBY_ROOM_NUMBER_FILE':d+'/counter','LOBBY_INITIAL_ROOM_NUMBER':'100'}):
                self.assertEqual(runtime_settings.next_room_number(28000),100)
                runtime_settings.save_next_room_number(102)
                self.assertEqual(runtime_settings.next_room_number(28000),102)

if __name__=='__main__':unittest.main()
