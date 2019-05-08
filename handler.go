// Copyright (c) 2019 sparetimecoders
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package goamqp

import (
	"fmt"
	"reflect"
)

func inputType(handler interface{}) reflect.Type {
	method, _ := reflect.TypeOf(handler).MethodByName("Process")
	return method.Type.In(1)
}

func getProcessFunction(handler interface{}) reflect.Value {
	m, _ := getProcessMethod(handler)
	return m.Func
}

func checkMessageHandler(handler interface{}) error {
	if err := checkHandler(handler); err != nil {
		return err
	}

	if err := checkOutputparameters(handler, 1); err != nil {
		return err
	}
	return nil
}

func checkRequestResponseHandlerReturns(handler interface{}) error {
	if err := checkHandler(handler); err != nil {
		return err
	}

	if err := checkOutputparameters(handler, 2); err != nil {
		return err
	}
	return nil
}

func checkOutputparameters(handler interface{}, expected int) error {
	methodType, _ := getProcessMethod(handler)
	handlerType := reflect.TypeOf(handler).Elem()

	if methodType.Type.NumOut() != expected {
		return fmt.Errorf("handler %s has incorrect number of return values. Expected %d, actual %d", handlerType, expected, methodType.Type.NumOut())
	}
	return nil
}

func checkHandler(handler interface{}) error {
	if handler == nil {
		return fmt.Errorf("handler is nil")
	}
	if reflect.TypeOf(handler).Kind() != reflect.Ptr {
		return fmt.Errorf("handler %s is not a pointer", reflect.TypeOf(handler))
	}
	handlerType := reflect.TypeOf(handler).Elem()

	m, ok := getProcessMethod(handler)
	if !ok {
		return fmt.Errorf("handler %s is missing Process method", handlerType)
	}

	methodType := m.Type
	if methodType.NumIn() != 2 {
		return fmt.Errorf("handler %s has incorrect number of arguments, expected 1 but was %d", handlerType, methodType.NumIn()-1)
	}

	return nil
}

func getProcessMethod(handler interface{}) (m reflect.Method, ok bool) {
	return reflect.TypeOf(handler).MethodByName("Process")
}
