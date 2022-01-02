import yaml

from dbd.db.db_schema import DbSchema


def test_schema_validation():
    with open('./tests/fixtures/schemas/schema1.yaml', 'r') as f:
        code = yaml.safe_load(f.read())
        result, errors = DbSchema.validate_code(code)
        assert result
    with open('./tests/fixtures/schemas/schema2.yaml', 'r') as f:
        code = yaml.safe_load(f.read())
        result, errors = DbSchema.validate_code(code)
        assert not result
    with open('./tests/fixtures/schemas/schema3.yaml', 'r') as f:
        code = yaml.safe_load(f.read())
        result, errors = DbSchema.validate_code(code)
        assert not result
    with open('./tests/fixtures/schemas/schema4.yaml', 'r') as f:
        code = yaml.safe_load(f.read())
        result, errors = DbSchema.validate_code(code)
        assert not result
    with open('./tests/fixtures/schemas/schema5.yaml', 'r') as f:
        code = yaml.safe_load(f.read())
        result, errors = DbSchema.validate_code(code)
        assert not result
