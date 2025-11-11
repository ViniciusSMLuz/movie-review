import express from 'express';
import bodyParser from 'body-parser';
import cassandra from 'cassandra-driver';
import path from 'path';

const DATABASE_HOST = process.env.CASSANDRA_HOST || '127.0.0.1'; // '127.0.0.1' como fallback
const DATABASE_DATACENTER = process.env.CASSANDRA_DATACENTER || 'datacenter1';
const DB_KEYSPACE = 'film_reviews_db';

const UUIDFactory = cassandra.types.Uuid;

const databaseClient = new cassandra.Client({
    contactPoints: [DATABASE_HOST],
    localDataCenter: DATABASE_DATACENTER,
    // policies: {
    //     retry: new cassandra.policies.retry.RetryPolicy()
    // },
    // queryOptions: { consistency: cassandra.types.consistencies.localQuorum }
});

// --- "Migration" Programática: Criar Keyspace e Tabelas ---

async function setupDatabase() {
    try {
        console.log("Conectando ao Cassandra...");
        await databaseClient.connect();
        console.log(`Conectado a ${databaseClient.hosts.length} hosts do cluster.`);

        console.log("Criando Keyspace (se não existir)...");
        await databaseClient.execute(`
            CREATE KEYSPACE IF NOT EXISTS ${DB_KEYSPACE}
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        `);

        // Informa ao databaseClient para usar este keyspace em todas as queries futuras
        databaseClient.keyspace = DB_KEYSPACE;

        console.log("Criando tabela 'films' (se não existir)...");
        await databaseClient.execute(`
            CREATE TABLE IF NOT EXISTS films (
                id uuid PRIMARY KEY,
                name text
            )
        `);

        console.log("Criando tabela 'movie_reviews' (se não existir)...");
        await databaseClient.execute(`
            CREATE TABLE IF NOT EXISTS movie_reviews (
                film_id uuid,
                created_at timestamp,
                review_uuid uuid,
                username text,
                score int,
                PRIMARY KEY (film_id, created_at, review_uuid)
            ) WITH CLUSTERING ORDER BY (created_at DESC)
        `);

        console.log("Banco de dados pronto!");

    } catch (err) {
        console.error("ERRO AO INICIALIZAR O BANCO:", err);
        process.exit(1); // Encerra a aplicação se não conseguir conectar/criar tabelas
    }
}

// --- Endpoints da API ---
const application = express();

application.use(bodyParser.json());

application.use(express.static(path.join(import.meta.dirname, '/public')));

// 1. Carregar a lista de filmes
application.get('/api/films', async (req, res) => {
    try {
        const cqlQuery = 'SELECT id, name FROM films';
        const dbResult = await databaseClient.execute(cqlQuery);
        res.json(dbResult.rows);
    } catch (err) {
        console.error("Erro ao buscar filmes:", err);
        res.status(500).json({ error: 'Erro ao buscar filmes' });
    }
});

// 2. Cadastrar um novo filme
application.post('/api/films', async (req, res) => {
    try {
        const { name } = req.body;
        if (!name) {
            return res.status(400).json({ error: 'O campo "name" é obrigatório' });
        }
        
        const filmId = UUIDFactory.random(); // Gera um UUID v4
        const cqlQuery = 'INSERT INTO films (id, name) VALUES (?, ?)';
        
        await databaseClient.execute(cqlQuery, [filmId, name], { prepare: true });
        
        // Retorna o filme criado com o ID gerado
        res.status(201).json({ id: filmId, name });

    } catch (err) {
        console.error("Erro ao adicionar filme:", err);
        res.status(500).json({ error: 'Erro ao adicionar filme' });
    }
});

// 3. Exibir a lista de avaliações de um filme
application.get('/api/films/:film_id/reviews', async (req, res) => {
    try {
        const { film_id } = req.params;
        const cqlQuery = 'SELECT film_id, username, score, created_at FROM movie_reviews WHERE film_id = ?';
        
        const dbResult = await databaseClient.execute(cqlQuery, [film_id], { prepare: true });
        
        res.json(dbResult.rows);

    } catch (err) {
        console.error("Erro ao buscar avaliações:", err);
        res.status(500).json({ error: 'Erro ao buscar avaliações' });
    }
});

// 4. Avaliar (armazenar avaliação) um filme
application.post('/api/films/:film_id/reviews', async (req, res) => {
    try {
        const { film_id } = req.params;
        const { username, score } = req.body;

        if (!username || score === undefined) {
            return res.status(400).json({ error: 'Campos "username" e "score" são obrigatórios' });
        }

        const created_at = new Date(); // Data atual
        const reviewUuid = UUIDFactory.random(); // ID único para a avaliação
        const scoreValue = parseInt(score, 10);

        const cqlQuery = 'INSERT INTO movie_reviews (film_id, created_at, review_uuid, username, score) VALUES (?, ?, ?, ?, ?)';
        
        await databaseClient.execute(cqlQuery, [film_id, created_at, reviewUuid, username, scoreValue], { prepare: true });

        res.status(201).json({ 
            film_id, 
            created_at, 
            review_uuid: reviewUuid, 
            username, 
            score: scoreValue 
        });

    } catch (err) {
        console.error("Erro ao enviar avaliação:", err);
        res.status(500).json({ error: 'Erro ao enviar avaliação' });
    }
});

const appPort = process.env.PORT || 3000;

// --- Iniciar o Servidor ---
// Primeiro inicializa o DB, depois inicia o servidor Express
setupDatabase().then(() => {
    application.listen(appPort, () => {
        console.log(`Servidor rodando em http://localhost:${appPort}`);
    });
}).catch(err => {
    console.error("Falha ao iniciar o servidor:", err);
});