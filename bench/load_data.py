from datasets import load_dataset

# 1. Load both 'train' and 'test' splits together
# The '+' operator concatenates them into one dataset
dataset = load_dataset("neo4j/text2cypher-2024v1", split="train+test")

# 2. Open a file and write each cypher query to a new line
output_file = "all_cypher_queries.txt"

with open(output_file, "w", encoding="utf-8") as f:
    for row in dataset:
        # Extract the 'cypher' column
        query = row["cypher"]

        # Strip any existing newlines within the query to ensure
        # it stays on a single line in your text file
        clean_query = query.replace("\n", " ").strip()

        f.write(clean_query + "\n")

print(f"Done! Saved {len(dataset)} queries to {output_file}")
