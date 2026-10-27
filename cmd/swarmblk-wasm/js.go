//go:build js && wasm

package main

import (
	"errors"
	"syscall/js"
)

// ignore is a callback that does nothing, for promises nobody waits for.
var ignore = js.FuncOf(func(js.Value, []js.Value) any { return nil })

// promise runs fn in a goroutine and returns a JavaScript promise of its
// result.
func promise(fn func() (any, error)) js.Value {
	executor := js.FuncOf(func(_ js.Value, args []js.Value) any {
		resolve, reject := args[0], args[1]
		go func() {
			v, err := fn()
			if err != nil {
				reject.Invoke(js.Global().Get("Error").New(err.Error()))
				return
			}
			resolve.Invoke(v)
		}()
		return nil
	})
	defer executor.Release() // the Promise constructor has called it
	return js.Global().Get("Promise").New(executor)
}

// await waits for a JavaScript promise and returns its value, or its
// rejection as an error.
func await(p js.Value) (js.Value, error) {
	type result struct {
		v   js.Value
		err error
	}
	c := make(chan result, 1)
	ok := js.FuncOf(func(_ js.Value, args []js.Value) any {
		c <- result{v: args[0]}
		return nil
	})
	fail := js.FuncOf(func(_ js.Value, args []js.Value) any {
		c <- result{err: errors.New(args[0].Call("toString").String())}
		return nil
	})
	defer ok.Release()
	defer fail.Release()
	p.Call("then", ok, fail)
	r := <-c
	return r.v, r.err
}

func toJS(b []byte) js.Value {
	a := js.Global().Get("Uint8Array").New(len(b))
	js.CopyBytesToJS(a, b)
	return a
}

// fromJS copies an ArrayBuffer or a Uint8Array.
func fromJS(v js.Value) []byte {
	a := v
	if !v.InstanceOf(js.Global().Get("Uint8Array")) {
		a = js.Global().Get("Uint8Array").New(v)
	}
	b := make([]byte, a.Get("length").Int())
	js.CopyBytesToGo(b, a)
	return b
}

// cache is a Cache from the browser's Cache Storage, which keeps responses
// under URLs: here, chunks, and writable disks' state and logs.
type cache struct{ c js.Value }

func openCache(name string) (cache, error) {
	c, err := await(js.Global().Get("caches").Call("open", name))
	return cache{c}, err
}

// put keeps body under key, with headers, which may be nil.
func (c cache) put(key string, body []byte, headers map[string]any) error {
	init := map[string]any{}
	if headers != nil {
		init["headers"] = headers
	}
	_, err := await(c.c.Call("put", key, js.Global().Get("Response").New(toJS(body), init)))
	return err
}

// putLater is put without waiting: a get before it lands finds nothing.
func (c cache) putLater(key string, body []byte) {
	c.c.Call("put", key, js.Global().Get("Response").New(toJS(body))).Call("catch", ignore)
}

// get returns what key holds and a header of it, or ok false.
func (c cache) get(key, header string) (body []byte, value string, ok bool, err error) {
	resp, err := await(c.c.Call("match", key))
	if err != nil || !resp.Truthy() {
		return nil, "", false, err
	}
	if header != "" {
		if h := resp.Get("headers").Call("get", header); h.Truthy() {
			value = h.String()
		}
	}
	buf, err := await(resp.Call("arrayBuffer"))
	if err != nil {
		return nil, "", false, err
	}
	return fromJS(buf), value, true, nil
}

func (c cache) remove(key string) error {
	_, err := await(c.c.Call("delete", key))
	return err
}

// keys returns the paths of the URLs the cache holds.
func (c cache) keys() ([]string, error) {
	reqs, err := await(c.c.Call("keys"))
	if err != nil {
		return nil, err
	}
	keys := make([]string, reqs.Length())
	for i := range keys {
		keys[i] = js.Global().Get("URL").New(reqs.Index(i).Get("url")).Get("pathname").String()
	}
	return keys, nil
}
