from app.binary_reader import BinaryReader


class TopicData:
    @classmethod
    def dump(cls, file_path: str) -> bytes:
        if not file_path:
            return bytes()

        with BinaryReader(file_path) as reader:
            return reader.read_all()
