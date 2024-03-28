import re
import res


def parse_rule_file(path):
    rule_data = {}

    s3 = res.connectors.load("s3")
    with open(path) if "s3://" not in path else s3.file_object(path, mode="r") as f:
        lines = f.readlines()
        dpoints = []
        size_list = None
        # Parse header information
        for line in lines:
            if line.startswith("ASTM/DXF"):
                rule_data["version"] = int(line.split(":")[1].strip())
            elif line.startswith("CREATION DATE"):
                rule_data["creation_date"] = line.split(":")[1].strip()
            elif line.startswith("CREATION TIME"):
                rule_data["creation_time"] = line.split(":")[1].strip()
            elif line.startswith("AUTHOR"):
                rule_data["author"] = line.split(":")[1].strip()
            elif line.startswith("UNITS"):
                rule_data["units"] = line.split(":")[1].strip()
            elif line.startswith("GRADE RULE TABLE"):
                rule_data["grade_rule_table"] = line.split(":")[1].strip()
            elif line.startswith("NUMBER OF SIZES"):
                rule_data["num_sizes"] = int(line.split(":")[1].strip())
            elif line.startswith("SIZE LIST"):
                size_list = list(map(str, line.split(":")[1].strip().split()))
                rule_data["size_list"] = size_list
            elif line.startswith("SAMPLE SIZE"):
                rule_data["sample_size"] = int(line.split(":")[1].strip())
            elif line.startswith("RULE:"):
                rule_name = line.split(":")[1].strip()
                rule_points = []
                for subline in lines[lines.index(line) + 1 :]:
                    if subline.startswith("RULE:"):
                        break
                    else:
                        points = re.findall(r"[-+]?\d*\.\d+|[-+]?\d+", subline)
                        rule_points.extend(
                            [
                                (float(points[i]), float(points[i + 1]))
                                for i in range(0, len(points), 2)
                            ]
                        )
                # rule_data[rule_name] = rule_points
                d = dict(zip(size_list, rule_points))
                d["id"] = rule_name

                dpoints.append(d)

        rule_data["points"] = dpoints

    return rule_data
