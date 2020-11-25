import json
import ast

# the main idea here is
# 1. parse the Rule ELM tree in JSON to an object,
# 2. get each field parts, such as value sets and rules,
# 3. convert them into each AST Tree Node using Python's in-built AST library, and
# 4. walk the tree


class CqlElmTreeParser:
    def __init__(self):
        self.elm = None

    def get_elm_from_json_string(self, json_str):
        self.elm = json.loads(json_str)
        return self.elm

    def get_elm_from_json_file(self, json_file_path):
        with open(json_file_path) as file:
            self.elm = json.load(file)
        return self.elm

    def parse_identifier(self):
        if self.elm is not None:
            return self.elm['library'].get('identifier')
        else:
            return None

    # parse the identifier from JSON data to an AST node
    def create_node_identifier(self):
        identifier = self.parse_identifier()
        identifier_node = ast.parse(json.dumps(identifier))
        return identifier_node

    def parse_schema_identifier(self):
        if self.elm is not None:
            return self.elm['library'].get('schemaIdentifier')
        else:
            return None

    def parse_usings(self):
        if self.elm is not None:
            return self.elm['library'].get('usings').get('def')
        else:
            return None

    def parse_parameters(self):
        if self.elm is not None:
            return self.elm['library'].get('parameters').get('def')
        else:
            return None

    # parse the parameters from JSON data to an AST node
    def create_node_parameters(self):
        parameters = self.parse_parameters()
        parameters_node = ast.parse(json.dumps(parameters))
        return parameters_node

    def parse_value_sets(self):
        if self.elm is not None:
            return self.elm['library'].get('valueSets').get('def')
        else:
            return None

    # parse the value_sets from JSON data to an AST node
    def create_node_value_sets(self):
        value_sets = self.parse_value_sets()
        value_sets_node = ast.parse(json.dumps(value_sets))
        return value_sets_node

    def parse_statements(self):
        if self.elm is not None:
            return self.elm['library'].get('statements').get('def')
        else:
            return None

    # parse the statements from JSON data to an AST node
    def create_node_statements(self):
        statements = self.parse_statements()
        statements_node = ast.parse(statements)
        return statements_node

    # parse a specific rule with a rule_name from JSON data to an AST node
    def create_node_rule(self, rule_name):
        statements = self.parse_statements()
        # filter statements defines to rules by applying rule name string
        rule = [rule for rule in statements if rule['name'] == rule_name]
        rule_node = ast.parse(json.dumps(rule))
        return rule_node


def main():
    # rule_json_file_path = './BreastCancerScreening-8.4.000.json'
    # parser = CqlElmTreeParser()
    # elm = parser.get_elm_from_json_file(rule_json_file_path)
    #
    # print("JSON: " + json.dumps(elm) + "\n")

    # identifier_node = parser.create_node_identifier()
    # print(ast.dump(identifier_node, indent=4) + "\n")
    # for n in ast.walk(identifier_node):
    #    print(ast.dump(n, indent=4) + "\n")

    # print("AST Node created for parameters")
    # parameters_node = parser.create_node_parameters()
    # print(ast.dump(parameters_node, indent=4) + "\n")
    # print(ast.walk(parameters_node))

    # print("AST Node created for valueSets")
    # value_sets_node = parser.create_node_value_sets()
    # print(ast.dump(value_sets_node, indent=4) + "\n")
    # print(ast.walk(value_sets_node))

    # from American Cancer Society's Recommendations for the Early Detection of Breast Cancer
    # https://www.cancer.org/cancer/breast-cancer/screening-tests-and-early-detection/american-cancer-society-recommendations-for-the-early-detection-of-breast-cancer.html
    # "Women 45 to 54 should get mammograms every year."
    cql_rule = "ShouldGetMammogramsEveryYear = Patient.gender == 'female' \
        and AgeInYears >= 45 and AgeInYears <= 54"
    print(cql_rule)
    mammograms_rule_ast = ast.parse(cql_rule)
    print(ast.dump(mammograms_rule_ast, indent=4) + "\n")

    print("Walking the AST Node for rule - 'Women 45 to 54 should get mammograms every year'")
    for n in ast.walk(mammograms_rule_ast):
        print(ast.dump(n, indent=4) + "\n")

    # use the pyspark - build a spark tree using the column_specs and call .select(*column_specs)

    # "statements" field is the one that has Rules defined,
    # print("AST Node created for a simple rule - InDemographic (from CQL direct)")
    # rule_in_python = "InDemographic = AgeInYearsAt >= 16 and AgeInYearsAt < 24 \
    #     and Patient.gender in FemaleAdministrativeSex"
    # rule_node_direct = ast.parse(rule_in_python)
    # print(ast.dump(rule_node_direct, indent=4) + "\n")

    # "statements" field is the one that has Rules defined,
    # print("AST Node created for a simple rule - InDemographic (from JSON)")
    # rule_node = parser.create_node_rule("InDemographic")
    # print(ast.dump(rule_node, indent=4) + "\n")

    # to visualize this rule - InDemographic,
    # I have used this web page - https://vpyast.appspot.com/
    # I have entered this "translated" Python code in there
    # InDemographic = AgeInYearsAt >= 16 and AgeInYearsAt < 24 and Patient.gender in FemaleAdministrativeSex

    # print("Walking the AST Node for rule - InDemographic (from JSON)")
    # for n in ast.walk(rule_node):
    #     print(ast.dump(n, indent=4) + "\n")

    # print(ast.literal_eval(tree)) # for evaluations on the Literal type Node for values.




if __name__ == "__main__":
    main()
