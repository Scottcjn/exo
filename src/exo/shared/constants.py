import os
from pathlib import Path

EXO_HOME_RELATIVE_PATH = os.environ.get("EXO_HOME", ".exo")
EXO_HOME = Path.home() / EXO_HOME_RELATIVE_PATH

EXO_MODELS_DIR_ENV = os.environ.get("EXO_MODELS_DIR")
EXO_MODELS_DIR = Path(EXO_MODELS_DIR_ENV) if EXO_MODELS_DIR_ENV else EXO_HOME / "models"

EXO_LOG = EXO_HOME / "exo.log"
EXO_CONFIG_FILE = EXO_HOME / "config.toml"

EXO_NODE_ID_KEYPAIR = EXO_HOME / "node_id.keypair"

# libp2p topics for event forwarding
LIBP2P_LOCAL_EVENTS_TOPIC = "worker_events"
LIBP2P_GLOBAL_EVENTS_TOPIC = "global_events"
LIBP2P_ELECTION_MESSAGES_TOPIC = "election_message"
LIBP2P_COMMANDS_TOPIC = "commands"
