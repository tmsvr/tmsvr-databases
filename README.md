# TMSVR Database experiments

Implementing database storage engines and related code to experiment

## Getting Started

This repository contains database engine implementations for demonstration purposes. The code is created as the basis of blog posts on my website.

The corresponding articles:
- [Log-structured merge trees](https://tmsvr.com/how-a-log-structured-merge-tree-database-engine-works/)
- [B-Trees](https://tmsvr.com/building-a-database-b-trees/)
- [Using generics](https://tmsvr.com/practical-use-of-generics-in-java/ )

The concept is simple, there is a `DataStore` interface that is implemented by different engines. The project uses minimal to zero dependencies, mainly for testing.

## Authors

- **Imre Tomosvari** - [TMSVR.com](https://tmsvr.com)

## License

This project is licensed under the [MIT License](LICENSE)
