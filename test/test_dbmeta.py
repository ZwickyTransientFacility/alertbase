from alertbase.dbmeta import DBMeta


class TestDBMeta:
    def test_to_file_roundtrip(self, tmp_path):
        filename = "meta.json"
        dbm = DBMeta("bucket", "region")
        with open(tmp_path / filename, "w") as f:
            dbm.write_to_file(f)
        with open(tmp_path / filename, "r") as f:
            have = DBMeta.read_from_file(f)
        assert have == dbm
