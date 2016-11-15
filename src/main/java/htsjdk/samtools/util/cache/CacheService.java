package htsjdk.samtools.util.cache;

import java.util.HashMap;
import java.util.Map;

public class CacheService {

    private static CacheService instance;

    synchronized public static CacheService instance() {
        if (instance == null) {
            instance = new CacheService();
        }
        return instance;
    }

    private Map<String, Cache> caches;

    private CacheService() {
        caches = new HashMap<>();
    }

    /**
     * Creates a {@link Cache} in the {@code CacheService}.
     * @param alias - the alias under which the cache will be created
     * @param keyType - the {@link Cache} key class
     * @param valueType - the {@link Cache} value class
     * @param size - size of cache
     * @param <K> - the type of the keys used to access getValue within this cache
     * @param <V> - the type of the values held within this cache
     * @return the created and initialized {@link Cache}
     * @throws java.lang.IllegalArgumentException - If there is already a cache registered with the given alias.
     */
    public synchronized <K,V> Cache<K, V> createCache(java.lang.String alias,
                                             java.lang.Class<K> keyType,
                                             java.lang.Class<V> valueType,
                                             int size) {
        if (caches.containsKey(alias)) {
            throw new IllegalArgumentException("There is already a cache registered with the " +
                    "given alias (\"" + alias + "\")");
        }

        final Cache<K, V> newCache = new Cache<>(keyType, valueType, size);
        caches.put(alias, newCache);
        return newCache;
    }

    /**
     * Retrieves the {@link Cache} associated with the given alias if one is known.
     *
     * @param alias - the alias under which the cache will be created
     * @param keyType - the {@link Cache} key class
     * @param valueType - the {@link Cache} value class
     * @param <K> - the type of the keys used to access getValue within this cache
     * @param <V> - the type of the values held within this cache
     * @return the {@link Cache} associated with the given alias, {@code null} if no association exists
     * @throws RuntimeException - If the keyType or valueType do not match the ones with
     * which the {@link Cache} was created
     */
    @SuppressWarnings(value = "unchecked")
    public synchronized <K,V> Cache<K, V> getCache(java.lang.String alias,
                                 java.lang.Class<K> keyType,
                                 java.lang.Class<V> valueType) {
        Cache cache = caches.get(alias);
        if (cache == null) {
            return null;
        }

        if (!(cache.getKeyType().isAssignableFrom(keyType) &&
                cache.getValueType().isAssignableFrom(valueType))) {
            throw new RuntimeException("Cache with name \"" + alias + "\" already exists with different " +
                    "type of key or value (" + cache.getKeyType() + ", " + cache.getValueType() + ")");
        }

        return (Cache<K, V>) cache;
    }

    /**
     * Removes the {@link Cache} associated with the alias provided, if oe is known.
     * @param alias - the alias for which to remove the {@link Cache}
     */
    public synchronized void removeCache(String alias) {
        caches.remove(alias);
    }
}
