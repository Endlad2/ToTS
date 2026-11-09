#ifndef TOTS_H
#define TOTS_H
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

typedef struct Handle Handle;
typedef struct TotsBuffer { uint8_t *ptr; size_t len; size_t cap; } TotsBuffer;

Handle *tots_engine_new(const char *uri);
void tots_engine_free(Handle *handle);
char *tots_config_json(const char *uri);
TotsBuffer tots_engine_encapsulate_ip(Handle *handle, uint16_t assoc_id, const uint8_t *data, size_t len);
TotsBuffer tots_engine_decapsulate(Handle *handle, const uint8_t *data, size_t len);
void tots_buffer_free(TotsBuffer buffer);
void tots_string_free(char *value);

#ifdef __cplusplus
}
#endif
#endif
