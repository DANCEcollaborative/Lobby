"""Exercise the real request handler without starting its destructive DB initializer."""
import ast
from contextlib import nullcontext, redirect_stdout
from datetime import datetime
import io
from pathlib import Path
from types import SimpleNamespace as NS
import unittest

class ParticipationTests(unittest.TestCase):
    def call(self, mode=None, existing=None):
        data=dict(name='Test',email='test@example.invalid',password='test@example.invalid',entityId='fcds-p2-26-fall-1a')
        if mode is not None:data['participationMode']=mode
        created=[]
        class User:
            query=NS(filter_by=lambda **kw:NS(first=lambda:existing))
            def __init__(self,**kw):
                self.__dict__.update(kw);self.start_time=datetime.now();created.append(self)
        def queued(pair):pair[0].code=200;pair[0].url='test-room-url'
        session=NS(add=lambda u:None,commit=lambda:None)
        env=dict(request=NS(get_json=lambda **kw:data),MODULE_SLUG='fcds-p2-26-fall-1a',nextThreadNum=0,
                 threading=NS(Event=lambda:NS(wait=lambda:None),Thread=NS),eventMapping={},threadMapping={},
                 thread_lock=nullcontext(),email_to_dns=lambda email:'test-at-example-invalid',NAMESPACE='default',
                 app=NS(app_context=nullcontext),User=User,session=session,lobby_db=NS(session=session),
                 user_queue=NS(put=queued),datetime=datetime,is_duplicate_user=lambda info,user:True)
        tree=ast.parse((Path(__file__).parents[1]/'lobby.py').read_text())
        fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='getJupyterlabUrl');fn.decorator_list=[]
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'lobby.py','exec'),env)
        with redirect_stdout(io.StringIO()):result=env['getJupyterlabUrl']()
        return result,created,env
    def test_solo_preference_is_saved_on_new_user(self):
        result,created,_=self.call('solo');self.assertEqual(result,'test-room-url');self.assertEqual(created[0].participation_mode,'solo')
    def test_old_clients_default_to_group(self):
        _,created,_=self.call();self.assertEqual(created[0].participation_mode,'group')
    def test_invalid_mode_never_queues(self):
        result,created,env=self.call('invalid');self.assertEqual(result[1],400);self.assertEqual(created,[]);self.assertEqual(env['eventMapping'],{})
    def test_existing_group_cannot_silently_reopen_as_solo(self):
        old=NS(participation_mode='group')
        result,created,env=self.call('solo',old)
        self.assertEqual(result[1],409);self.assertIn('Join a group',result[0]['detail']);self.assertEqual(created,[]);self.assertEqual(env['eventMapping'],{})
    def test_existing_solo_reconnects_without_changing_preference(self):
        old=NS(participation_mode='solo',user_id='test-at-example-invalid')
        result,created,_=self.call('solo',old);self.assertEqual(result,'test-room-url');self.assertEqual(created,[])

if __name__=='__main__':unittest.main()
