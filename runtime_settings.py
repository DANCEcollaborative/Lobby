"""Optional deployment-specific settings; legacy installations keep their defaults."""
import os
from pathlib import Path


def setting(name, default):
    raw = os.environ.get('LOBBY_' + name)
    if raw is None:
        return default
    if isinstance(default, bool):
        if raw.lower() not in {'true', 'false'}:
            raise ValueError('LOBBY_' + name + ' must be true or false')
        return raw.lower() == 'true'
    if isinstance(default, int):
        value = int(raw)
        if value < 0:
            raise ValueError('LOBBY_' + name + ' must be nonnegative')
        return value
    return raw


def next_room_number(default):
    path = os.environ.get('LOBBY_ROOM_NUMBER_FILE')
    if path and Path(path).exists():
        return int(Path(path).read_text().strip())
    return setting('INITIAL_ROOM_NUMBER', default)


def save_next_room_number(value):
    """Reserve the next number before scheduling; one assigner owns this file."""
    raw = os.environ.get('LOBBY_ROOM_NUMBER_FILE')
    if not raw:
        return
    path = Path(raw)
    temp = path.with_name(path.name + '.new')
    with open(temp, 'w') as output:
        output.write(str(value) + '\n')
        output.flush()
        os.fsync(output.fileno())
    os.replace(temp, path)
