import express from 'express';
import bodyParser from 'body-parser';
import cassandra from 'cassandra-driver';
import path from 'path';

const DB_HOST = process.env.CASSANDRA_HOST || '127.0.0.1'; // '127.0.0.1' como fallback
const DB_DATACENTER = process.env.CASSANDRA_DATACENTER || 'datacenter1';
const DATABASE_KEYSPACE = 'cinema_ratings';

const UUIDGenerator = cassandra.types.Uuid;

const dbClient = new cassandra.Client({
    contactPoints: [DB_HOST],
    localDataCenter: DB_DATACENTER,
    // policies: {
    //     retry: new cassandra.policies.retry.RetryPolicy()
    // },
    // queryOptions: { consistency: cassandra.types.consistencies.localQuorum }
});

// --- "Migration" Programática: Criar Keyspace e Tabelas ---

async function initializeDatabase() {
    try {
        console.log("Conectando ao Cassandra...");
        await dbClient.connect();
        console.log(`Conectado a ${dbClient.hosts.length} hosts do cluster.`);

        console.log("Criando Keyspace (se não existir)...");
        await dbClient.execute(`
            CREATE KEYSPACE IF NOT EXISTS ${DATABASE_KEYSPACE}
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        `);

        // Informa ao dbClient para usar este keyspace em todas as queries futuras
        dbClient.keyspace = DATABASE_KEYSPACE;

        console.log("Criando tabela 'movies' (se não existir)...");
        await dbClient.execute(`
            CREATE TABLE IF NOT EXISTS movies (
                id uuid PRIMARY KEY,
                title text
            )
        `);

        console.log("Criando tabela 'reviews_by_movie' (se não existir)...");
        await dbClient.execute(`
            CREATE TABLE IF NOT EXISTS reviews_by_movie (
                movie_id uuid,
                timestamp timestamp,
                review_id uuid,
                reviewer text,
                rating int,
                PRIMARY KEY (movie_id, timestamp, review_id)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
        `);

        console.log("Banco de dados pronto!");

    } catch (error) {
        console.error("ERRO AO INICIALIZAR O BANCO:", error);
        process.exit(1); // Encerra a aplicação se não conseguir conectar/criar tabelas
    }
}

// --- Endpoints da API ---
const server = express();

server.use(bodyParser.json());

server.use(express.static(path.join(import.meta.dirname, '/public')));

// 1. Carregar a lista de filmes
server.get('/api/movies', async (req, res) => {
    try {
        const sqlQuery = 'SELECT id, title FROM movies';
        const queryResult = await dbClient.execute(sqlQuery);
        res.json(queryResult.rows);
    } catch (error) {
        console.error("Erro ao buscar filmes:", error);
        res.status(500).json({ error: 'Erro ao buscar filmes' });
    }
});

// 2. Cadastrar um novo filme
server.post('/api/movies', async (req, res) => {
    try {
        const { title } = req.body;
        if (!title) {
            return res.status(400).json({ error: 'O campo "title" é obrigatório' });
        }
        
        const movieId = UUIDGenerator.random(); // Gera um UUID v4
        const sqlQuery = 'INSERT INTO movies (id, title) VALUES (?, ?)';
        
        await dbClient.execute(sqlQuery, [movieId, title], { prepare: true });
        
        // Retorna o filme criado com o ID gerado
        res.status(201).json({ id: movieId, title });

    } catch (error) {
        console.error("Erro ao adicionar filme:", error);
        res.status(500).json({ error: 'Erro ao adicionar filme' });
    }
});

// 3. Exibir a lista de avaliações de um filme
server.get('/api/movies/:movie_id/reviews', async (req, res) => {
    try {
        const { movie_id } = req.params;
        const sqlQuery = 'SELECT movie_id, reviewer, rating, timestamp FROM reviews_by_movie WHERE movie_id = ?';
        
        const queryResult = await dbClient.execute(sqlQuery, [movie_id], { prepare: true });
        
        res.json(queryResult.rows);

    } catch (error) {
        console.error("Erro ao buscar avaliações:", error);
        res.status(500).json({ error: 'Erro ao buscar avaliações' });
    }
});

// 4. Avaliar (armazenar avaliação) um filme
server.post('/api/movies/:movie_id/reviews', async (req, res) => {
    try {
        const { movie_id } = req.params;
        const { reviewer, rating } = req.body;

        if (!reviewer || rating === undefined) {
            return res.status(400).json({ error: 'Campos "reviewer" e "rating" são obrigatórios' });
        }

        const timestamp = new Date(); // Data atual
        const reviewId = UUIDGenerator.random(); // ID único para a avaliação
        const ratingValue = parseInt(rating, 10);

        const sqlQuery = 'INSERT INTO reviews_by_movie (movie_id, timestamp, review_id, reviewer, rating) VALUES (?, ?, ?, ?, ?)';
        
        await dbClient.execute(sqlQuery, [movie_id, timestamp, reviewId, reviewer, ratingValue], { prepare: true });

        res.status(201).json({ 
            movie_id, 
            timestamp, 
            review_id: reviewId, 
            reviewer, 
            rating: ratingValue 
        });

    } catch (error) {
        console.error("Erro ao enviar avaliação:", error);
        res.status(500).json({ error: 'Erro ao enviar avaliação' });
    }
});

const serverPort = process.env.PORT || 3000;

// --- Iniciar o Servidor ---
// Primeiro inicializa o DB, depois inicia o servidor Express
initializeDatabase().then(() => {
    server.listen(serverPort, () => {
        console.log(`Servidor rodando em http://localhost:${serverPort}`);
    });
}).catch(error => {
    console.error("Falha ao iniciar o servidor:", error);
});