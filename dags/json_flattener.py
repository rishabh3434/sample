class JsonFlattener:
    def __init__(self, separator="."):
        self.separator = separator

    def flatten(self, data, parent_key=""):
        result = {}

        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{parent_key}{self.separator}{key}" if parent_key else str(key)

                if isinstance(value, (dict, list)):
                    result.update(self.flatten(value, new_key))
                else:
                    result[new_key] = value

        elif isinstance(data, list):
            for index, item in enumerate(data):
                new_key = f"{parent_key}{self.separator}{index}" if parent_key else str(index)

                if isinstance(item, (dict, list)):
                    result.update(self.flatten(item, new_key))
                else:
                    result[new_key] = item

        else:
            if parent_key:
                result[parent_key] = data

        return result
