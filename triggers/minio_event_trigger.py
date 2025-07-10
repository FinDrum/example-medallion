import logging
from urllib.parse import unquote
from threading import Thread
from flask import Flask, request, jsonify
from findrum.interfaces import EventTrigger

logger = logging.getLogger("findrum")

class MinIOEventTrigger(EventTrigger):
    def __init__(self, **config):
        self.config = config
        self.app = Flask("minio-listener")
        self._setup_routes()

        self.port = config.get("port", 5000)
        self.bucket = config.get("bucket")
        self.prefix = config.get("prefix")
        self.suffix = config.get("suffix")
        self.emit = lambda data: None

    def _setup_routes(self):
        @self.app.route("/minio-event", methods=["POST"])
        def handle_event():
            try:
                data = request.get_json()
                logger.info(f"📥 Evento recibido: {data.get('EventName', 'unknown')}")

                for record in data.get("Records", []):
                    bucket = record["s3"]["bucket"]["name"]
                    key = unquote(record["s3"]["object"]["key"])
                    logger.info(f"🔍 Detectado archivo: {bucket}/{key}")

                    if self.bucket and bucket != self.bucket:
                        logger.info(f"⏭ Ignorado por filtro de bucket: {bucket}")
                        continue
                    if self.prefix and not key.startswith(self.prefix):
                        logger.info(f"⏭ Ignorado por filtro de prefix: {key}")
                        continue
                    if self.suffix and not key.endswith(self.suffix):
                        logger.info(f"⏭ Ignorado por filtro de suffix: {key}")
                        continue

                    event_data = {
                        "event_source": "minio",
                        "bucket": bucket,
                        "file_path": key,
                    }

                    logger.info(f"🚀 Emitting evento: {event_data}")
                    self.emit(event_data)

                return jsonify({"status": "success"}), 200

            except Exception as e:
                logger.error(f"❌ Error procesando evento MinIO: {str(e)}")
                return jsonify({"error": str(e)}), 500

    def start(self):
        logger.info(f"🚀 Iniciando MinIOEventTrigger en el puerto {self.port}")
        thread = Thread(target=self._run_server, args=(self.port,), daemon=True)
        thread.start()
        return thread

    def _run_server(self, port):
        self.app.run(
            host="0.0.0.0",
            port=port,
            debug=False,
            use_reloader=False
        )