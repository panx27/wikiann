import sys
import os
import logging
import argparse
from configparser import ConfigParser
from pymongo import MongoClient
import ujson as json
from collections import defaultdict
import multiprocessing
import subprocess
import pprint
from src.sentences import get_sent
from src.utils import *
from src.encoder import Encoder
from src.vector_store import VectorStore
import numpy as np
import io
import torch
import faiss
import pickle
import functools
from scipy.spatial.distance import cosine


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


# ==============================================================================
# From https://github.com/facebookresearch/MUSE/blob/master/src/utils.py

FAISS_AVAILABLE = True
FAISS_CPU = True

def load_vec(emb_path, nmax=1e9):
    vectors = []
    word2id = {}
    logger.info('loading %s' % emb_path)
    with io.open(emb_path, 'r', encoding='utf-8',
                 newline='\n', errors='ignore') as f:
        next(f)
        for i, line in enumerate(f):
            word, vect = line.rstrip().split(' ', 1)
            vect = np.fromstring(vect, sep=' ')
            assert word not in word2id, 'word found twice'
            vectors.append(vect)
            word2id[word] = len(word2id)
            if len(word2id) == nmax:
                break
    logger.info("loaded %i pre-trained embeddings." % len(vectors))
    id2word = {v: k for k, v in word2id.items()}
    word_vec = {}
    embeddings = np.vstack(vectors)
    for i in word2id:
        word_vec[i] = embeddings[word2id[i]]
    return embeddings, id2word, word2id, word_vec


def get_nn_avg_dist(emb, query, knn):
    """
    Compute the average distance of the `knn` nearest neighbors
    for a given set of embeddings and queries.
    Use Faiss if available.
    """
    if FAISS_AVAILABLE:
        emb = emb.cpu().numpy()
        query = query.cpu().numpy()
        # if hasattr(faiss, 'StandardGpuResources'):
        if not FAISS_CPU:
            # gpu mode
            res = faiss.StandardGpuResources()
            config = faiss.GpuIndexFlatConfig()
            config.device = 0
            index = faiss.GpuIndexFlatIP(res, emb.shape[1], config)
        else:
            # cpu mode
            index = faiss.IndexFlatIP(emb.shape[1])
        index.add(emb)
        distances, _ = index.search(query, knn)
        return distances.mean(1)
    else:
        bs = 1024
        all_distances = []
        emb = emb.transpose(0, 1).contiguous()
        for i in range(0, query.shape[0], bs):
            distances = query[i:i + bs].mm(emb)
            best_distances, _ = distances.topk(knn, dim=1,
                                               largest=True, sorted=True)
            all_distances.append(best_distances.mean(1).cpu())
        all_distances = torch.cat(all_distances)
    return all_distances.numpy()


def csls_knn(queries, keys):
    knn = min(5, len(queries), len(keys))
    # normalize embeddings
    queries = torch.from_numpy(queries).float()
    queries = queries / queries.norm(2, 1, keepdim=True).expand_as(queries)
    keys = torch.from_numpy(keys).float()
    keys = keys / keys.norm(2, 1, keepdim=True).expand_as(keys)
    # average distances to k nearest neighbors
    average_dist_keys = torch.from_numpy(get_nn_avg_dist(queries, keys, knn))
    average_dist_queries = torch.from_numpy(get_nn_avg_dist(keys, queries, knn))
    # scores
    scores = keys.mm(queries.transpose(0, 1)).transpose(0, 1)
    scores.mul_(2)
    scores.sub_(average_dist_queries[:, None].float() + \
                average_dist_keys[None, :].float())
    scores = scores.cpu()
    top_matches = scores.topk(knn, 1, True)
    return top_matches


def bow(sentences, vec_store, normalize=False):
    """
    Get sentence representations using average bag-of-words.
    """
    vec_word = vec_store.get_vector_by_item
    vec_idx = vec_store.get_vector_by_index
    embeddings = []
    for sent in sentences:
        sentvec = [vec_word(w) for w in sent if vec_word(w) is not None]
        if normalize:
            sentvec = [v / np.linalg.norm(v) for v in sentvec]
        if len(sentvec) == 0:
            sentvec = [np.ones(vec_idx(0).shape)]
        embeddings.append(np.mean(sentvec, axis=0))
    return np.vstack(embeddings)


def bow_idf(sentences, vec_store, idf_dict=None):
    """
    Get sentence representations using weigthed IDF bag-of-words.
    """
    vec_word = vec_store.get_vector_by_item
    vec_idx = vec_store.get_vector_by_index
    embeddings = []
    for sent in sentences:
        sent = set(sent)
        words = [w for w in sent if vec_word(w) is not None and w in idf_dict]
        if len(words) > 0:
            sentvec = [vec_word(w) * idf_dict[w] for w in words]
            sentvec = sentvec / np.sum([idf_dict[w] for w in words])
        else:
            sentvec = [np.ones(vec_idx(0).shape)]
        embeddings.append(np.sum(sentvec, axis=0))
    return np.vstack(embeddings)


# ==============================================================================


def get_vectors(lang, inses, vec_store, ll_lang=None):
    encoder = Encoder(lang) if lang in Encoder.langs else None
    sents = []
    mids = []
    for i in inses:
        sent = get_sent(lang, i, ll_lang=ll_lang, encoder=encoder,
                        keep_sent_wo_link=True, keep_link_wo_ll=True)
        sent = list(sent)[0]
        if not sent:
            continue
        sents.append(sent)
        mids.append(str(i['_id']))

    idf = defaultdict(int)
    for s in sents:
        for w in set(s):
            idf[w] += 1
    for w in idf:
        idf[w] = max(1, np.log10(len(sents) / idf[w]))

    # vectors = bow(sents, vec_store)
    vectors = bow_idf(sents, vec_store, idf)
    return sents, vectors, idf, mids


def process(lang_src, lang_tgt, queries, outpath):
    client = MongoClient(host=host, port=port)
    coll_name_src = '%s_ll_%s'  % (lang_src, lang_tgt)
    coll_name_tgt = '%s'  % (lang_tgt)
    coll_src = client[db_name][coll_name_src]
    coll_tgt = client[db_name][coll_name_tgt]
    vs_src = VectorStore(host, port, 'vector_store',
                         'xling_%s-%s' % (lang_src, lang_tgt))
    vs_tgt = VectorStore(host, port, 'vector_store',
                         'mono_%s' % (lang_tgt))

    with open(outpath, 'w') as fw:
        for query in queries:
            print(query)
            res = {'source_id_ll': query, 'matches': {}}
            inses_src = coll_src.find({'source_id_ll': query})
            r_src = get_vectors(lang_src, inses_src, vs_src, ll_lang=lang_tgt)
            sents_src, vectors_src, idf_src, _ids_src = r_src
            inses_tgt = coll_tgt.find({'source_id': query})
            r_tgt = get_vectors(lang_tgt, inses_tgt, vs_tgt)
            sents_tgt, vectors_tgt, idf_tgt, _ids_tgt = r_tgt

            matches = csls_knn(vectors_src, vectors_tgt)
            for n, pair in enumerate(zip(*matches)):
                res['matches'][_ids_src[n]] = []
                for score, m in zip(*pair):
                    res['matches'][_ids_src[n]].append((score, _ids_tgt[m]))
            fw.write(json.dumps(res) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('pconf', help='Path to config file')
    parser.add_argument('lang_src', help='Source language')
    parser.add_argument('lang_tgt', help='Target language')
    parser.add_argument('outdir', help='Output directory')
    parser.add_argument('--nworker', '-n', default=1,
                        help='Number of workers (default=1)')
    args = parser.parse_args()

    config_path = args.pconf
    config = ConfigParser()
    config.read(config_path)
    host = config.get('mongodb', 'host')
    port = config.getint('mongodb', 'port')
    client = MongoClient(host=host, port=port)
    db_name = config.get('mongodb', 'db_name')

    nworker = int(args.nworker)
    os.makedirs(args.outdir, exist_ok=True)

    # pool = multiprocessing.Pool(processes=int(nworker))
    # logger.info('# of workers: %s' % nworker)
    # tol = client[db_name][lang_src].find(query).count()
    # logger.info('# of matched sentences: %s' % tol)
    # chunk_size = int(tol / nworker)
    # if chunk_size == 0:
    #     chunk_size = 1
    # logger.info('chunk size: %s' % chunk_size)
    # chunks = []
    # for i in range(0, tol, chunk_size):
    #     chunks.append((i, i+chunk_size))

    # logger.info('processing...')
    # results = []
    # for i in chunks:
    #     a = (lang_src, lang_tgt, query, i, args.outdir,)
    #     pool.apply_async(process, args=a,)
    # pool.close()
    # pool.join()

    # logger.info('merging...')
    # cmds = [
    #     'cat %s/*.tmp > %s/%s-%s.json' % (args.outdir, args.outdir, lang_src, lang_tgt),
    #     'rm %s/*.tmp' % (args.outdir),
    # ]
    # for cmd in cmds:
    #     subprocess.call(cmd, shell=True)

    # lang_src = 'zh'
    # lang_tgt = 'en'
    # p_vec_src = '/nas/data/m1/panx2/lib/MUSE/dumped/xling/ff3gop1kh5/vectors-zh.txt'
    # p_vec_tgt = '/nas/data/m1/panx2/tmp/wikiann/embeddings/en.wiki.word2vec.dim300.min5.vec'
    # # _, _, _, vec_src = load_vec(p_vec_src, nmax=5000)
    # # _, _, _, vec_tgt = load_vec(p_vec_tgt, nmax=5000)
    # _, _, _, vec_src = load_vec(p_vec_src)
    # _, _, _, vec_tgt = load_vec(p_vec_tgt)

    # # queries = ['Naples']
    # q = {'source_id_ll': {'$exists': True}}
    # queries = client[db_name][lang_src].find(q).distinct('source_id_ll')
    # queries = queries[:10000]
    # outpath = '%s/%s-%s.pickle' % (outdir, lang_src, lang_tgt)
    # process(lang_src, lang_tgt, queries, outpath)

    # queries = ['55880']
    # queries = ['1000053']
    # queries = ['37284']
    # queries = ['426208']
    # queries = ['426208', '37284', '1000053', '55880']

    coll_name_src = '%s_ll_%s'  % (args.lang_src, args.lang_tgt)
    chunks = client[db_name][coll_name_src].distinct('_chunk_id')
    for i in chunks:
        print(i)
        q = {'_chunk_id': i, 'source_id_ll': {'$exists': True}}
        queries = client[db_name][coll_name_src].find(q).distinct('source_id_ll')
        queries = queries[:10000]
        outpath = '%s/%s-%s.tmp' % (args.outdir, args.lang_src, args.lang_tgt)
        process(args.lang_src, args.lang_tgt, queries, outpath)
        break
