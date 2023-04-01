## Update April 2023

This project is currently being rewritten to act as a fully ASGI/RSGI compliant application framework layer, eliminating the current forced choice of using aiohttp as a web server. Instead, it focuses on developing a secure, high-performance, production-ready web engine for data-driven applications. The overarching goal of this project is to deliver a streamlined and excellent developer experience for complex web apps. To achieve this goal, it presupposes external dependencies for the data store and cache layers, defaulting to PostgreSQL and Redis, respectively.

Furthermore, this project does away with the MVC/T monolith pattern, automatically turning defined models (schemas in Postgres parlance) into ready-to-use views and controllers. This pattern still works for more complex projects by allowing arbitrary extension of the generated views. What this achieves is the ability to vary responses to the same payloads depending on the requesting actors' access privileges.

More details to follow.
