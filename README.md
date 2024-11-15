# sqlfluff-sqlmesh-templater

A custom templater for SQLFluff designed to preprocess SQL scripts used with SQLMesh. This templater extracts the SELECT statement, ignores the MODEL block and pre/post hooks, and removes `@` symbols. This is convenient because SQLMesh scripts are otherwise valid SQL, and this allows for seamless linting and formatting with SQLFluff.

## Features

- Extract SELECT Statement: Identifies and processes only the SELECT statement in the SQL script, ignoring other statements like MODEL, pre-hooks, post-hooks, etc.
- Remove `@` Symbols: Strips out all `@` symbols outside of comments and string literals.
- Accurate Parsing: Utilizes a super simple tokenizer to reasonably parse SQL scripts, ensuring that comments and string literals are handled correctly.
- Integration with SQLFluff CLI: Designed to be used directly with the SQLFluff command-line interface (CLI) without the need for additional Python code.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Example](#example)
- [How It Works](#how-it-works)
- [Contributing](#contributing)
- [License](#license)

## Installation

You can install the sqlfluff-sqlmesh-templater directly from the Git repository using pip:

`pip install git+https://github.com/z3z1ma/sqlfluff-sqlmesh-templater.git`

## Usage

To use the templater with SQLFluff, configure SQLFluff to use this custom templater by updating your .sqlfluff configuration file.

### Configuration

Create or update your .sqlfluff configuration file with the following content:

```toml
[tool.sqlfluff.core]
templater = "sqlmesh"
```

Note:

- The templater module should be accessible to SQLFluff. Ensure that the module is installed in your Python environment where SQLFluff can find it.

### Example

Given an input SQL script example.sql:

```sql
/*This is a comment with the word select*/
MODEL (
    name "silver"."active_developers",
    kind VIEW,
    owner 'PL',
    tags array[silver, active_developers, idp],
    grains (active_developer_id, concat(account_id, project_id, user_id)),
    audits array[not_null(columns := array[account_id, user_id]), unique_values(columns := array[active_developer_id])],
    enabled @feature_flag('MODULE_IDP')
);

SELECT
    @generate_surrogate_key(account_id, project_id, user_id) AS active_developer_id,
    account_id,
    org_id,
    project_id,
    user_id,
    created_time,
    last_updated_time,
    last_accessed_time,
    email, /* The hashed email of the developer */
    user_name, /* The hashed username of the developer*/
    is_deleted
FROM "staging"."platform-harness-idp-activeDevelopers";

VACUUM @this_model;
```

When you run SQLFluff with the custom templater:

`sqlfluff lint example.sql`

The templater processes the script, and SQLFluff lints only the SELECT statement with the `@` symbols removed:

```sql
SELECT
    generate_surrogate_key(account_id, project_id, user_id) AS active_developer_id,
    account_id,
    org_id,
    project_id,
    user_id,
    created_time,
    last_updated_time,
    last_accessed_time,
    email, /* The hashed email of the developer */
    user_name, /* The hashed username of the developer*/
    is_deleted
FROM "staging"."platform-harness-idp-activeDevelopers"
```

## How It Works

(this is under the hood)

The templater works by tokenizing the input SQL script and performing the following transformations:

 1. Tokenization: The script is tokenized to identify comments, strings, identifiers, keywords, symbols, and whitespace. This helps in accurately parsing the script without being misled by SELECT keywords or `@` symbols inside comments or string literals.
 2. Identifying the SELECT Statement: It searches for the SELECT statement outside of comments and strings, regardless of case, and starts processing from there.
 3. Removing `@` Symbols: During processing, any `@` symbols found outside of comments and strings are removed.
 4. Ignoring Non-SELECT Statements: The templater ignores the MODEL block, pre-hooks, and post-hooks.
 5. Processing Until Semicolon or End of Script: The templater continues processing tokens until it encounters the first semicolon ; after the SELECT statement or reaches the end of the script.
 6. Constructing the TemplatedFile: It constructs a TemplatedFile object that maps the original script to the transformed script, which SQLFluff can then lint and format.

## Why This Templater?

SQLMesh scripts often include templating constructs and special symbols (like `@`) that are not standard SQL. However, the core of a SQLMesh model is valid SQL, especially the SELECT statements. This templater allows SQLFluff to focus on the actual SQL code by stripping away the non-SQL parts, making it easier to lint and format your SQL scripts without running into parsing errors.

In the future, we may use a more sophisticated parser or tokenizer. The only reason we do not use SQLGlot is due to it not storing string positions, which is necessary for the templater to work.

## Contributing

Contributions are welcome! If you’d like to contribute to this project, please follow these steps:

 1. Fork the Repository: Click the “Fork” button at the top of the repository page.
 2. Install the Repository Locally:

`pip install -e git+https://github.com/z3z1ma/sqlfluff-sqlmesh-templater.git#egg=sqlfluff-sqlmesh-templater`

 3. Create a Feature Branch:

`git checkout -b feature/your-feature-name`

 4. Make Your Changes: Implement your feature or bug fix.
 5. Commit Your Changes:

`git commit -am 'Add new feature'`

 6. Push to Your Fork:

`git push origin feature/your-feature-name`

 7. Submit a Pull Request: Open a pull request to the main repository’s main branch.

Please ensure your code follows the existing style and includes appropriate tests.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

Feel free to open an issue or submit a pull request if you encounter any problems or have suggestions for improvements.

Note: For any queries or support, please contact Alex Butler.
