"""Test the actual handler's validation with Flask, without importing DB startup code."""
import ast
from pathlib import Path
import unittest
from flask import Flask, request

SOURCE = Path(__file__).resolve().parents[1] / 'lobby.py'

class RequestParsingTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse(SOURCE.read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'getJupyterlabUrl')
        fn.decorator_list = []
        # Stop at the first success-path print, before events, DB access or queuing.
        end = next(i for i, n in enumerate(fn.body) if isinstance(n, ast.Expr)
                   and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name)
                   and n.value.func.id == 'print')
        fn.body = fn.body[:end] + [ast.Return(value=ast.Name(id='data', ctx=ast.Load()))]
        module = ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[]))
        env = {'request': request, 'SOLO_CHOICE': True}
        exec(compile(module, str(SOURCE), 'exec'), env)
        self.app = Flask(__name__)
        self.app.add_url_rule('/getJupyterlabUrl', view_func=env['getJupyterlabUrl'], methods=['POST'])
        self.client = self.app.test_client()
        self.payload = dict(name='Test User', email='test@example.invalid', password='test-secret', entityId='test-entity')

    def test_json_and_legacy_plain_text_preserve_fields(self):
        import json
        for content_type in ('application/json', 'application/json;charset=UTF-8',
                             'application/vnd.example+json', 'text/plain;charset=UTF-8'):
            with self.subTest(content_type=content_type):
                response = self.client.post('/getJupyterlabUrl', data=json.dumps(self.payload), content_type=content_type)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.get_json(), self.payload)

    def test_malformed_empty_and_non_object_json_are_400(self):
        for content_type in ('application/json', 'text/plain;charset=UTF-8'):
            for body in ('', '{bad', 'null', '[]', '"hello"', 'false', '1'):
                with self.subTest(content_type=content_type, body=body):
                    response = self.client.post('/getJupyterlabUrl', data=body, content_type=content_type)
                    self.assertEqual(response.status_code, 400)

    def test_required_fields_must_be_nonempty_strings(self):
        for field in self.payload:
            for value in (None, '', '  ', 123, [], {}):
                payload = dict(self.payload, **{field: value})
                response = self.client.post('/getJupyterlabUrl', json=payload)
                self.assertEqual(response.status_code, 400)
                self.assertIn(field, response.get_json()['detail'])
            payload = dict(self.payload)
            del payload[field]
            self.assertEqual(self.client.post('/getJupyterlabUrl', json=payload).status_code, 400)

    def test_unsupported_content_types_are_415(self):
        for content_type in ('application/x-www-form-urlencoded', 'text/html', None):
            response = self.client.post('/getJupyterlabUrl', data='email=test', content_type=content_type)
            self.assertEqual(response.status_code, 415)

    def test_invalid_participation_mode_is_400(self):
        response = self.client.post('/getJupyterlabUrl', json=dict(self.payload, participationMode='invalid'))
        self.assertEqual(response.status_code, 400)

if __name__ == '__main__':
    unittest.main()
