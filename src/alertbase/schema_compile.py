from typing import Dict, Any, Callable, IO

import fastavro._read  # noqa

# mypy: ignore_errors


def compile_schema(schema: Dict[str, Any]) -> Callable[[IO[bytes]], Dict[str, Any]]:
    code = compile(
        source=code_for_schema(schema, "compiled_schema_parser"),
        filename="<autocompiled>",
        mode="exec",
    )
    eval(code)
    return locals()["compiled_schema_parser"]


def code_for_schema(schema, funcname):
    code = f"def {funcname}(src):\n"
    code += "    obj = {'root': {}}\n"
    indent = 1
    path = ["root"]
    for field in schema.fields:
        try:
            code += code_for_field(field.name, field.type, indent, path)
        except Exception:
            print(code)
            raise
    code += "    return obj['root']\n"
    print(code)
    return code


def code_for_field(name, schema, indent, path):
    lhs = "obj" + "".join(f'["{pathname}"]' for pathname in path) + f'["{name}"]'

    # Primitive types
    if schema.type == "string":
        rhs = "fastavro._read.read_utf8(src)"
    elif schema.type == "long":
        rhs = "fastavro._read.read_long(src)"
    elif schema.type == "double":
        rhs = "fastavro._read.read_double(src)"
    elif schema.type == "int":
        rhs = "fastavro._read.read_long(src)"
    elif schema.type == "boolean":
        rhs = "fastavro._read.read_boolean(src)"
    elif schema.type == "float":
        rhs = "fastavro._read.read_float(src)"
    elif schema.type == "bytes":
        rhs = "fastavro._read.read_bytes(src)"
    elif schema.type == "null":
        return "    " * indent + "pass\n"

    # Complex types
    elif schema.type == "record":
        rhs = "{}"
        code = ("    " * indent) + lhs + " = " + rhs + "\n"
        path.append(name)
        for field in schema.fields:
            code += code_for_field(field.name, field.type, indent, path)
        path.pop()
        return code
    elif schema.type == "union":
        code = ("    " * indent) + "idx = fastavro._read.read_long(src)\n"
        for i, union_schema in enumerate(schema.schemas):
            if i == 0:
                code += ("    " * indent) + f"if idx == {i}:\n"
            else:
                code += ("    " * indent) + f"elif idx == {i}:\n"
            code += code_for_field(name, union_schema, indent + 1, path)
        return code
    elif schema.type == "array":
        code = "    " * indent + lhs + " = []\n"
        code += "    " * indent + "blocksize = fastavro._read.read_long(src)\n"
        code += "    " * indent + "while blocksize > 0:\n"
        indent += 1
        code += "    " * indent + "for _ in range(blocksize):\n"
        indent += 1
        # Use a temporary value hanging off the root object, just for convenience.
        code += code_for_field("__tmpval", schema.items, indent, path)
        tmpval_name = (
            "obj" + "".join(f'["{pathname}"]' for pathname in path) + '["__tmpval"]'
        )

        code += "    " * indent + lhs + f".append({tmpval_name})\n"
        code += "    " * indent + f"del {tmpval_name}\n"
        indent -= 1
        code += "    " * indent + "blocksize = fastavro._read.read_long(src)\n"
        return code
    # TODO: Fixed type, map type
    else:
        raise ValueError("unexpected type: " + schema.type)
    return ("    " * indent) + lhs + " = " + rhs + "\n"
